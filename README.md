Cómo usar Kafka (parte de Fabián)

1. docker-compose up -d
2. docker exec -it arquitecturalambda-kafka-1 kafka-topics --create --topic log_de_eventos --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
3. pip install kafka-python
4. python producer.py 500     # para datos iniciales
   o python producer.py       # para flujo en tiempo real

Broker: localhost:9092
Topic: log_de_eventos

Formato de cada mensaje (JSON):
{
  "id": "uuid",
  "timestamp": "2026-02-25T18:13:45.123456",
  "data": {
    "event_type": "agregar_carrito | eliminar_carrito | modificar_carrito | view_product | compra",
    "user_id": 42,
    "product_id": 1050,          // para la mayoría
    "quantity": 3,               // agregar/modificar
    "items": [ {...}, {...} ],   // solo para compra
    "total": 127.50              // solo para compra
  }
}