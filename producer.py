import json
from kafka import KafkaProducer
from datetime import datetime
import uuid
import random
import time
import sys

# Configuración Kafka (igual que el workshop)
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 0, 0),          # ← add this line
    retries=5,
    request_timeout_ms=10000,
    metadata_max_age_ms=300000,
    max_block_ms=10000,
    connections_max_idle_ms=540000
)

# Datos de prueba
event_types = ["agregar_carrito", "eliminar_carrito", "modificar_carrito", "view_product", "compra"]
users = list(range(1, 51))      # 50 usuarios simulados
products = list(range(1001, 1101))  # 100 productos simulados

def generate_data(event_type):
    base = {
        "event_type": event_type,
        "user_id": random.choice(users)
    }
    if event_type in ["agregar_carrito", "modificar_carrito"]:
        base["product_id"] = random.choice(products)
        base["quantity"] = random.randint(1, 5)
    elif event_type == "eliminar_carrito":
        base["product_id"] = random.choice(products)
    elif event_type == "view_product":
        base["product_id"] = random.choice(products)
    elif event_type == "compra":
        num_items = random.randint(1, 4)
        items = [
            {"product_id": random.choice(products), "quantity": random.randint(1, 3)}
            for _ in range(num_items)
        ]
        base["items"] = items
        base["total"] = round(sum(random.uniform(10, 100) * item["quantity"] for item in items), 2)
    return base

# ====================== EJECUCIÓN ======================
if len(sys.argv) > 1 and sys.argv[1].isdigit():
    num_events = int(sys.argv[1])
    print(f"Generando {num_events} eventos de una sola vez...")
    for _ in range(num_events):
        event_type = random.choice(event_types)
        event = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "data": generate_data(event_type)
        }
        producer.send('log_de_eventos', value=event)
        print(f"✅ Enviado → {event_type} | user {event['data']['user_id']}")
    producer.flush()
    print("¡Todos los eventos enviados!")
else:
    print("🚀 Modo tiempo real activado (Ctrl + C para parar)")
    while True:
        event_type = random.choice(event_types)
        event = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "data": generate_data(event_type)
        }
        producer.send('log_de_eventos', value=event)
        producer.flush()   # para que se vea inmediatamente
        print(f"✅ Enviado → {event_type} | user {event['data']['user_id']}")
        time.sleep(random.uniform(0.5, 3.0))  # simula eventos en tiempo real