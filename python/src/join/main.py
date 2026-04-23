import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        # client_id -> [partial_top]
        self.partial_tops_by_client = {}

        # client_id -> cantidad de tops parciales recibidos
        # Cuando llega a AGGREGATION_AMOUNT se mergea y envia el top final
        self.top_count_by_client = {}

        self.shutting_down = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Joiner: SIGTERM recibido, cerrando...")
        self.shutting_down = True
        self.input_queue.stop_consuming()

    def _merge_and_send(self, client_id):
        partial_tops = self.partial_tops_by_client.pop(client_id)
        del self.top_count_by_client[client_id]

        # Cada Aggregator acumula un subconjunto disjunto de frutas
        # por lo que el merge es simplemente juntar y ordenar
        all_items = []
        for partial_top in partial_tops:
            for fruit, amount in partial_top:
                all_items.append(fruit_item.FruitItem(fruit, amount))

        all_items.sort(reverse=True)
        final_top = [
            (item.fruit, item.amount)
            for item in all_items[:TOP_SIZE]
        ]

        logging.info(f"Joiner: enviando top final para cliente {client_id}")
        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) != 2:
            logging.error(f"Joiner: mensaje inesperado: {fields}")
            nack()
            return

        client_id, partial_top = fields

        if client_id not in self.partial_tops_by_client:
            self.partial_tops_by_client[client_id] = []
            self.top_count_by_client[client_id] = 0

        self.partial_tops_by_client[client_id].append(partial_top)
        self.top_count_by_client[client_id] += 1

        count = self.top_count_by_client[client_id]
        logging.info(f"Joiner: top {count}/{AGGREGATION_AMOUNT} para cliente {client_id}")

        if count == AGGREGATION_AMOUNT:
            self._merge_and_send(client_id)

        ack()

    def _close(self):
        self.input_queue.close()
        self.output_queue.close()

    def start(self):
        self.input_queue.start_consuming(self.process_message)
        self._close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()
    return 0


if __name__ == "__main__":
    main()