import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{AGGREGATION_PREFIX}_{ID}"
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        # client_id -> [FruitItem] ordenado por amount
        self.fruit_top_by_client = {}

        # client_id -> cantidad de EOFs recibidos de instancias de Sum
        # Cuando llega a SUM_AMOUNT se calcula y envia el top parcial
        self.eof_count_by_client = {}

        self.shutting_down = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info(f"Aggregation {ID}: SIGTERM recibido, cerrando...")
        self.shutting_down = True
        self.input_queue.stop_consuming()

    def _process_data(self, client_id, fruit, amount):
        if client_id not in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []

        fruit_top = self.fruit_top_by_client[client_id]
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                # Al actualizar el amount hay que reordenar la lista
                # extrayendo el item y reinsertar con bisect
                updated = fruit_top[i] + fruit_item.FruitItem(fruit, amount)
                fruit_top.pop(i)
                bisect.insort(fruit_top, updated)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        self.eof_count_by_client[client_id] = (
            self.eof_count_by_client.get(client_id, 0) + 1
        )
        count = self.eof_count_by_client[client_id]
        logging.info(f"Aggregation {ID}: EOF {count}/{SUM_AMOUNT} para cliente {client_id}")

        if count < SUM_AMOUNT:
            return

        fruit_top = self.fruit_top_by_client.pop(client_id, [])
        del self.eof_count_by_client[client_id]

        chunk = list(fruit_top[-TOP_SIZE:])
        chunk.reverse()
        partial_top = [(item.fruit, item.amount) for item in chunk]

        logging.info(f"Aggregation {ID}: enviando top parcial para cliente {client_id}")
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, partial_top])
        )

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        elif len(fields) == 1:
            self._process_eof(fields[0])
        else:
            logging.error(f"Aggregation {ID}: mensaje inesperado: {fields}")
            nack()
            return
        ack()

    def _close(self):
        self.input_queue.close()
        self.output_queue.close()

    def start(self):
        self.input_queue.start_consuming(self.process_message)
        self._close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()