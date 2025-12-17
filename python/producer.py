# producer.py
import json
import time
from kafka import KafkaProducer

# Ajusta bootstrap_servers si hiciera falta (localhost:9092 o host.docker.internal:9092)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

topic = "mi_tema"

def send_messages(n=10, pause=0.5):
    for i in range(n):
        payload = {"id": i, "mensaje": f"Hola Kafka desde Python {i}"}
        future = producer.send(topic, value=payload)
        # opcional: esperar confirmación (blocking)
        try:
            record_metadata = future.get(timeout=10)
            print(f"Enviado a {record_metadata.topic} [partición {record_metadata.partition}] offset {record_metadata.offset}: {payload}")
        except Exception as e:
            print("Error enviando:", e)
        time.sleep(pause)

if __name__ == "__main__":
    send_messages(2000000, pause=0.5)
    producer.flush()
    producer.close()
