import os
import logging
import threading
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        # Receptor: escucha solo la routing key propia para recibir
        # notificaciones de EOF de cualquier par
        self.control_receiver = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{ID}"]
        )

        # Emisor: publica a todas las routing keys en un solo send(),
        # haciendo broadcast a todos los pares
        all_routing_keys = [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)]
        self.control_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, all_routing_keys
        )

        # Una cola por cada Aggregator para rutear frutas por hash
        self.aggregation_queues = []
        for i in range(AGGREGATION_AMOUNT):
            queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{AGGREGATION_PREFIX}_{i}"
            )
            self.aggregation_queues.append(queue)

        # client_id -> {fruit -> FruitItem}
        self.amount_by_fruit_by_client = {}

        # Condition para sincronizar el flush con el procesamiento de datos.
        # El thread de control espera a que no haya mensajes de datos en vuelo
        # antes de hacer flush, evitando que un dato llegue despues de la señal
        # de control del mismo cliente
        self.pending_data = 0
        self.pending_condition = threading.Condition(threading.Lock())

        self.shutting_down = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info(f"Sum {ID}: SIGTERM recibido, cerrando...")
        self.shutting_down = True
        with self.pending_condition:
            self.pending_condition.notify_all()
        self.input_queue.stop_consuming()
        self.control_receiver.stop_consuming()

    def _get_aggregator_index(self, fruit):
        # Funcion determinista que garantiza que la misma fruta siempre
        # va al mismo Aggregator sin importar que instancia de Sum la procese
        return sum(ord(c) for c in fruit) % AGGREGATION_AMOUNT

    def _process_data(self, client_id, fruit, amount):
        with self.pending_condition:
            self.pending_data += 1

        with self.pending_condition:
            if client_id not in self.amount_by_fruit_by_client:
                self.amount_by_fruit_by_client[client_id] = {}
            client_fruits = self.amount_by_fruit_by_client[client_id]
            client_fruits[fruit] = client_fruits.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            self.pending_data -= 1
            self.pending_condition.notify_all()

    def _broadcast_eof(self, client_id):
        self.control_sender.send(
            message_protocol.internal.serialize([client_id])
        )

    def _flush_to_aggregators(self, client_id):
        # Espera a que no haya mensajes de datos en procesamiento antes de
        # tomar el snapshot del estado acumulado para este cliente
        with self.pending_condition:
            self.pending_condition.wait_for(
                lambda: self.pending_data == 0 or self.shutting_down
            )
            if self.shutting_down:
                return
            client_fruits = self.amount_by_fruit_by_client.pop(client_id, {})

        for item in client_fruits.values():
            idx = self._get_aggregator_index(item.fruit)
            self.aggregation_queues[idx].send(
                message_protocol.internal.serialize(
                    [client_id, item.fruit, item.amount]
                )
            )
        for queue in self.aggregation_queues:
            queue.send(message_protocol.internal.serialize([client_id]))

    def _close(self):
        self.input_queue.close()
        self.control_receiver.close()
        self.control_sender.close()
        for queue in self.aggregation_queues:
            queue.close()

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
            ack()
        elif len(fields) == 1:
            self._broadcast_eof(fields[0])
            ack()
        else:
            logging.error(f"Sum {ID}: mensaje inesperado: {fields}")
            nack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        logging.info(f"Sum {ID}: flush para cliente {client_id}")
        self._flush_to_aggregators(client_id)
        ack()

    def start(self):
        # Dos threads corren en paralelo para consumir datos y señales de
        # control simultaneamente. Ambos son I/O bound por lo que el GIL
        # no limita el paralelismo real entre ellos. Necesitan compartir
        # estado (amount_by_fruit_by_client) por lo que se usan threads
        # en lugar de procesos
        control_thread = threading.Thread(
            target=self.control_receiver.start_consuming,
            args=(self.process_control_message,)
        )
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_message)
        control_thread.join()
        self._close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()