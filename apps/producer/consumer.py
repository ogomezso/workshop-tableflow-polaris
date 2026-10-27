import os

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

TOPIC = os.environ.get("TOPIC", "orders_events")

sr = SchemaRegistryClient({
    "url": os.environ["SR_URL"],
    "basic.auth.user.info": f'{os.environ["SR_KEY"]}:{os.environ["SR_SECRET"]}',
})

consumer = DeserializingConsumer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_KEY"],
    "sasl.password": os.environ["KAFKA_SECRET"],
    "group.id": os.environ.get("GROUP_ID", "orders-monitor"),
    "auto.offset.reset": "latest",
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": AvroDeserializer(sr),
})
consumer.subscribe([TOPIC])

count, total = 0, 0.0
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"consumer error: {msg.error()}", flush=True)
        continue
    order = msg.value()
    count += 1
    total += order["amount"]
    if count % 20 == 0:
        print(f"orders={count} revenue={total:.2f} last_sku={order['sku']}", flush=True)
