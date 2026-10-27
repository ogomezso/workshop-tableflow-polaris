import os
import random
import time
import uuid

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

TOPIC = os.environ.get("TOPIC", "orders_events")

SCHEMA = """
{"type":"record","name":"OrderEvent","namespace":"demo",
 "fields":[{"name":"order_id","type":"string"},{"name":"customer_id","type":"int"},
           {"name":"sku","type":"string"},{"name":"qty","type":"int"},
           {"name":"amount","type":"double"},{"name":"event_ts","type":"long"}]}
"""

sr = SchemaRegistryClient({
    "url": os.environ["SR_URL"],
    "basic.auth.user.info": f'{os.environ["SR_KEY"]}:{os.environ["SR_SECRET"]}',
})

producer = SerializingProducer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_KEY"],
    "sasl.password": os.environ["KAFKA_SECRET"],
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": AvroSerializer(sr, SCHEMA),
})

SKUS = ["SKU-A", "SKU-B", "SKU-C", "SKU-D"]
RATE = float(os.environ.get("RATE_SECONDS", "1"))


def on_delivery(err, msg):
    if err is not None:
        print(f"delivery failed: {err}", flush=True)


while True:
    order = {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.randint(1, 5),
        "sku": random.choice(SKUS),
        "qty": random.randint(1, 5),
        "amount": round(random.uniform(5, 200), 2),
        "event_ts": int(time.time() * 1000),
    }
    producer.produce(TOPIC, key=order["order_id"], value=order, on_delivery=on_delivery)
    producer.poll(0)
    time.sleep(RATE)
