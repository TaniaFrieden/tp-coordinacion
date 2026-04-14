import pika
from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

# Tipo de exchange usado para enrutar mensajes por coincidencia exacta de routing key
EXCHANGE_TYPE_DIRECT = "direct"

# Cantidad maxima de mensajes no confirmados que RabbitMQ entrega a cada consumidor
# a la vez. Valor 1 garantiza un reparto equitativo entre consumidores concurrentes
PREFETCH_COUNT = 1


def _wrap_pika_callback(on_message_callback):
    """Adapta el callback de pika a la firma esperada por el middleware

    Args:
        on_message_callback: funcion con firma (message, ack, nack) provista por
            el usuario del middleware

    Returns:
        Funcion con la firma nativa de pika lista para pasarse a basic_consume
    """
    def _consume_callback(channel, method, properties, body):
        delivery_tag = method.delivery_tag

        def ack():
            channel.basic_ack(delivery_tag=delivery_tag)

        def nack():
            # requeue=True devuelve el mensaje a la cola para que otro
            # consumidor pueda procesarlo en caso de fallo transitorio.
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

        on_message_callback(body, ack, nack)

    return _consume_callback


class _BaseMiddlewareRabbitMQ:
    """Clase base con la logica de ciclo de vida comun a Queue y Exchange

    Centraliza los metodos que son identicos en ambas implementaciones concretas:
    liberacion de recursos, detencion del consumo y cierre de conexion
    """

    def _cleanup_resources(self):
        """Libera canal y conexion parcialmente inicializados sin relanzar errores
        """
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
        except pika.exceptions.AMQPError:
            pass

        try:
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPError:
            pass

    def start_consuming(self, on_message_callback):
        """Inicia el loop bloqueante de consumo sobre la cola asociada a esta instancia

        Args:
            on_message_callback: funcion con firma (message, ack, nack) que se
                invoca por cada mensaje recibido.

        Raises:
            MessageMiddlewareDisconnectedError: si se pierde la conexion durante el consumo
            MessageMiddlewareMessageError: ante cualquier otro error de AMQP
        """
        try:
            self.channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=_wrap_pika_callback(on_message_callback),
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while consuming from '{self._name}'"
            ) from exc
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareMessageError(
                f"Error while consuming from '{self._name}'"
            ) from exc
        finally:
            # Se limpia el tag independientemente de como termino el loop, para
            # que stop_consuming() no intente detener un consumer ya inexistente.
            self.consumer_tag = None

    def stop_consuming(self):
        """Detiene el loop de consumo si esta instancia habia registrado un consumer

        Raises:
            MessageMiddlewareDisconnectedError: si se pierde la conexion al detener
            MessageMiddlewareMessageError: ante cualquier otro error de AMQP
        """
        try:
            if (
                self.channel is not None
                and self.channel.is_open
                and self.consumer_tag is not None
            ):
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while stopping '{self._name}' consumer"
            ) from exc
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareMessageError(
                f"Error while stopping '{self._name}' consumer"
            ) from exc

    def close(self):
        """Cierra canal y conexion liberando todos los recursos del middleware

        Raises:
            MessageMiddlewareCloseError: si ocurre un error de AMQP al cerrar
        """
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareCloseError(
                f"Error while closing middleware '{self._name}'"
            ) from exc


class MessageMiddlewareQueueRabbitMQ(_BaseMiddlewareRabbitMQ, MessageMiddlewareQueue):
    """Implementacion de work queue sobre RabbitMQ

    Multiples instancias apuntando a la misma cola comparten los mensajes en
    round-robin. Cada instancia maneja su propia conexion y canal de forma
    independiente.
    """

    def __init__(self, host, queue_name):
        """Crea la conexion, el canal y asegura la existencia de la cola

        Args:
            host: direccion del broker RabbitMQ
            queue_name: nombre de la cola a declarar y usar

        Raises:
            MessageMiddlewareDisconnectedError: si no se puede conectar al broker
            MessageMiddlewareMessageError: si falla la declaracion de la cola
        """
        self.host = host
        self.queue_name = queue_name
        self._name = queue_name
        self.connection = None
        self.channel = None
        self.consumer_tag = None

        try:
            parameters = pika.ConnectionParameters(host=self.host)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except pika.exceptions.AMQPConnectionError as exc:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError(
                f"Failed to connect to RabbitMQ queue '{self.queue_name}'"
            ) from exc
        except pika.exceptions.AMQPError as exc:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError(
                f"Failed to declare queue '{self.queue_name}'"
            ) from exc

    def send(self, message):
        """Publica un mensaje en la cola usando el exchange por defecto de RabbitMQ.

        El exchange vacio ("") enruta el mensaje directamente a la cola cuyo
        nombre coincide con la routing_key, que es el mecanismo estandar de
        work queue en RabbitMQ

        Args:
            message: contenido del mensaje a publicar (bytes o str)

        Raises:
            MessageMiddlewareDisconnectedError: si se pierde la conexion al publicar
            MessageMiddlewareMessageError: ante cualquier otro error de AMQP
        """
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while sending to queue '{self.queue_name}'"
            ) from exc
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareMessageError(
                f"Error while sending to queue '{self.queue_name}'"
            ) from exc


class MessageMiddlewareExchangeRabbitMQ(_BaseMiddlewareRabbitMQ, MessageMiddlewareExchange):
    """Implementacion de exchange directo con cola exclusiva por consumidor.

    La cola se elimina automaticamente al cerrar la conexion (exclusive=True,
    auto_delete=True), por lo que no deja recursos huerfanos en el broker
    """

    def __init__(self, host, exchange_name, routing_keys):
        """Declara el exchange y crea una cola exclusiva bindeada a las routing keys

        Si el exchange ya existe con parametros compatibles, RabbitMQ lo reutiliza.
        Se puede pasar una o varias routing keys; la instancia recibira mensajes
        publicados con cualquiera de ellas.

        Args:
            host: direccion del broker RabbitMQ
            exchange_name: nombre del exchange directo a declarar
            routing_keys: lista de routing keys a las que suscribir esta instancia

        Raises:
            MessageMiddlewareDisconnectedError: si no se puede conectar al broker
            MessageMiddlewareMessageError: si falla la declaracion del exchange,
                la cola o los bindings
        """
        self.host = host
        self.exchange_name = exchange_name
        self._name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = None
        self.connection = None
        self.channel = None
        self.consumer_tag = None

        try:
            parameters = pika.ConnectionParameters(host=self.host)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type=EXCHANGE_TYPE_DIRECT,
            )

            # Cola anonima y exclusiva: RabbitMQ genera un nombre unico y la
            # elimina al cerrar la conexion. Necesaria para que cada consumidor
            # reciba su propia copia del mensaje (broadcast).
            queue_result = self.channel.queue_declare(
                queue="",
                exclusive=True,
                auto_delete=True,
            )
            self.queue_name = queue_result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )
        except pika.exceptions.AMQPConnectionError as exc:
            self._cleanup_resources()
            raise MessageMiddlewareDisconnectedError(
                f"Failed to connect to RabbitMQ exchange '{self.exchange_name}'"
            ) from exc
        except pika.exceptions.AMQPError as exc:
            self._cleanup_resources()
            raise MessageMiddlewareMessageError(
                f"Failed to initialize exchange '{self.exchange_name}'"
            ) from exc

    def send(self, message):
        """Publica el mensaje en el exchange por cada routing key configurada
        Args:
            message: contenido del mensaje a publicar (bytes o str)

        Raises:
            MessageMiddlewareDisconnectedError: si se pierde la conexion al publicar
            MessageMiddlewareMessageError: ante cualquier otro error de AMQP
        """
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while sending to exchange '{self.exchange_name}'"
            ) from exc
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareMessageError(
                f"Error while sending to exchange '{self.exchange_name}'"
            ) from exc