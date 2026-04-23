# Informe — TP Coordinación

## Coordinación entre instancias de Sum

Las instancias de Sum comparten una work queue (input_queue) desde la cual RabbitMQ distribuye los mensajes en round-robin. Cada instancia acumula independientemente los pares (fruta, cantidad) que recibe, indexados por client_id para soportar múltiples clientes concurrentes.

El problema de coordinación surge al momento del EOF: dado que RabbitMQ entrega el EOF a una sola instancia de Sum, las demás no se enteran de que la ingesta de un cliente terminó. Para resolverlo se utiliza un exchange directo de control. Cada instancia de Sum suscribe una cola exclusiva a su propia routing key ({SUM_PREFIX}_{ID}), y mantiene un emisor configurado con las routing keys de todos los pares. Cuando cualquier instancia recibe el EOF de un cliente, hace un único send() al emisor, RabbitMQ entrega una copia a cada Sum a través del exchange. De este modo todas las instancias reciben la señal con una sola conexión adicional, sin importar cuántas réplicas haya.

Para evitar el caso borde en que una señal de control llegue antes de que se termine de procesar un dato del mismo cliente, se utiliza un threading.Condition con un contador pending_data. El thread de control espera a que pending_data == 0 antes de hacer el flush, garantizando que todos los datos en vuelo fueron procesados.

### Uso de threads

Cada instancia de Sum utiliza dos threads: el thread principal consume la input_queue y el thread de control consume el exchange receptor. El uso de threads en lugar de procesos se justifica por dos razones:

1. Ambos loops son I/O bound: pasan la mayor parte del tiempo esperando mensajes de RabbitMQ, por lo que el GIL de Python se libera durante la espera y no limita el paralelismo real entre los dos threads.
2.  Ambos threads necesitan acceder al estado compartido amount_by_fruit_by_client. Si se implementaran como procesos separados, sería necesario usar mecanismos de comunicación entre procesos, lo que aumentaría la complejidad y el costo de sincronización.

El acceso concurrente al estado compartido está protegido por el mismo threading.Condition que sincroniza el flush.

## Coordinación entre instancias de Aggregator

Cada instancia de Aggregator escucha su propia work queue ({AGGREGATION_PREFIX}_{ID}). Las instancias de Sum rutean cada fruta siempre a la misma instancia de Aggregator usando una función de hash determinista:

```python
sum(ord(c) for c in fruit) % AGGREGATION_AMOUNT
```

Esto garantiza que cada fruta es acumulada por exactamente un Aggregator, eliminando redundancia de cómputo.

Se utiliza una work queue en lugar de un exchange para la comunicación Sum -> Aggregator porque garantiza orden FIFO dentro de cada conexión, los datos de un mismo Sum siempre llegan antes que su EOF al Aggregator.

Cada Aggregator cuenta los EOFs recibidos por cliente (eof_count_by_client). Cuando el contador alcanza SUM_AMOUNT, todos los Sum terminaron de enviar datos para ese cliente y el Aggregator puede calcular y enviar el top parcial al Joiner.

## Coordinación en el Joiner

El Joiner recibe tops parciales de todos los Aggregators, indexados por client_id. Cuando recibe AGGREGATION_AMOUNT tops parciales para un cliente, los mergea en el top final. Dado que cada Aggregator acumula un subconjunto disjunto de frutas, el merge consiste simplemente en juntar todas las frutas y ordenarlas, sin necesidad de sumar duplicados.

## Escalabilidad respecto a los clientes

Cada cliente que se conecta al gateway recibe un client_id único generado en MessageHandler.__init__(). Este ID viaja en todos los mensajes internos del sistema, permitiendo que Sum, Aggregator y Joiner procesen múltiples consultas concurrentes sin mezclar sus datos.

## Escalabilidad respecto a la cantidad de controles

La cantidad de instancias de Sum y Aggregator se configura mediante variables de entorno (SUM_AMOUNT, AGGREGATION_AMOUNT). El sistema usa estas variables para construir dinámicamente las colas y exchanges. Agregar réplicas solo requiere modificar el docker-compose.