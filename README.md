Cómo usar Kafka (parte de Fabián)

1. `docker-compose up -d`
2. `docker exec -it arquitecturalambda-kafka-1 kafka-topics --create --topic log_de_eventos --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1`
3. `pip install kafka-python`

- Para datos iniciales: `python producer.py <number_of_events>`
- Para flujo en tiempo real: `python producer.py`


Broker: localhost:9092, Topic: log_de_eventos

Formato de cada mensaje (JSON):

```json
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
```

------------------------------------------------------------
Cómo correr Spark + Data Lake (Cynthia)
------------------------------------------------------------

Requisitos:
- Java 17
- Spark 4.1.1 instalado (por ejemplo con Homebrew)
- Conector Kafka para Spark 4.1.1 (Scala 2.13):
  org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1

1) (Opcional pero recomendado) Crear carpetas del Data Lake
`mkdir -p batch checkpoints datalake/raw/events datalake/processed`

2) Verificar que Kafka está recibiendo eventos (opcional)
`docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic log_de_eventos --from-beginning --max-messages 5`

3) Ingesta RAW: Kafka -> datalake/raw/events (streaming)
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 batch/ingest_raw.py`

>Nota: Este script se queda corriendo para ir “bajando” eventos del tópico a Parquet. Para detenerlo usa Ctrl + C cuando ya tengas suficientes eventos.

4) Procesar métricas batch: RAW -> PROCESSED (Parquet)
`spark-submit batch/process_metrics.py`

Salida esperada:
- datalake/processed/events_by_type
- datalake/processed/revenue_per_day
- datalake/processed/top_products_by_qty
- datalake/processed/funnel_ratios

5) Verificar resultados con Spark Shell
`spark-shell`

Luego corre:

``` sh
spark.read.parquet("datalake/processed/events_by_type").show(false)
spark.read.parquet("datalake/processed/revenue_per_day").show(false)
spark.read.parquet("datalake/processed/top_products_by_qty").show(false)
spark.read.parquet("datalake/processed/funnel_ratios").show(false)
```


6) (Opcional) Limpiar todo y reiniciar desde cero
``` sh
rm -rf datalake checkpoints spark-warehouse
mkdir -p batch checkpoints datalake/raw/events datalake/processed
```

------------------------------------------------------------
Spark + InfluxDB (Tono)
------------------------------------------------------------
1) Crear topic de kafka
`docker exec -it kafka kafka-topics --create --topic log_de_eventos --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`
`docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092`

2) Procesar datos (Spark Structred Streaming)
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_kafka_speed.py`

Ingresar a la UI de InfluxDB (http://localhost:8086)
 - Organization: lambda_org
 - Bucket: lambda_speed
 - Token: modificar el archivo de 'spark_kafka_speed.py' para incluir el token creado



 ---
 ---
## Resumen de comandos

```sh
docker-compose down -v
docker-compose up -d
docker exec -it kafka kafka-topics --create --topic log_de_eventos --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
clear
```

**REVISAR QUE INFLUX TENGA LOS 2 BUCKETS**

InfluxDB (http://localhost:8086)
 - Username:     `admin`
 - Password:     `admin123`
 - Organization: `lambda_org`
 - Bucket:       `lambda_speed`, `lambda_batch`
 - Token:        `<TOKEN>`

> Cambiar INFLUX_TOKEN al token creado por InfluxDB en `serving_layer.py` y `spark_kafka_speed.py`

> Cambiar token al token creado por InfluxDB en `grafana_provisioning/datasources/dashboard.yml`

**BATCH LAYER**
1) `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 batch/ingest_raw.py`
2) `spark-submit batch/process_metrics.py`

**SPEED LAYER**
1) `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_kafka_speed.py`
 
**PRODUCER:** `python producer.py`

**SERVING LAYER**

Grafana (http://localhost:3000)
 - Username:     admin
 - Password:     admin

 `python serving_layer.py`
 > Esto debe correse despues de `spark -submit batch/process_metrics.py`