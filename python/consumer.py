# consumer.py
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "mi_tema",
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',   # empezar desde el principio si no hay offset
    enable_auto_commit=True,
    group_id="grupo-ejemplo",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Esperando mensajes en mi_tema... (Ctrl+C para salir)")

try:
    for msg in consumer:
        print(f"Recibido: {msg.value} (partición {msg.partition}, offset {msg.offset})")
except KeyboardInterrupt:
    print("Interrumpido por usuario")
finally:
    consumer.close()
