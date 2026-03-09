# spark_kafka_streaming.py
# Unifica el procesamiento batch y speed en un solo Spark Streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType
import os
from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime
 

# Configuración
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "log_de_eventos"
CHECKPOINT_LOCATION = "./checkpoints/streaming"
RAW_PATH = "./datalake/raw/events"

# InfluxDB config (from speed layer)
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "2jnFpAv9uNq9Z_3ynatuvQqFdGmmuSx4ecmVouwGXg8Q-S4BaMZm4ZJQFTJ3RsgOFLYIRovw_26Sc4eTCO080A=="
INFLUX_ORG = "lambda_org"
INFLUX_BUCKET = "lambda_speed"

# Esquema de los datos
schema = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("data", StructType([
        StructField("event_type", StringType()),
        StructField("user_id", IntegerType()),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("items", ArrayType(StructType([
            StructField("product_id", IntegerType()),
            StructField("quantity", IntegerType())
        ])), True),
        StructField("total", FloatType(), True)
    ]))
])

# Spark session
spark = SparkSession.builder \
    .appName("Lambda Unified Streaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Leer de Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parsear el valor de Kafka
json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")
parsed_df = json_df.select(from_json(col("json_value"), schema).alias("root"))

# Aplanar los datos
# Aplanar los datos

final_df = parsed_df.select(
    col("root.id").alias("id"),
    col("root.timestamp").alias("timestamp"),
    col("root.data.event_type").alias("event_type"),
    col("root.data.user_id").alias("user_id"),
    col("root.data.product_id").alias("product_id"),
    col("root.data.quantity").alias("quantity"),
    col("root.data.items").alias("items"),
    col("root.data.total").alias("total")
)

# Agregar columna event_time para InfluxDB
final_df = final_df.withColumn("event_time", to_timestamp(col("timestamp")))


# Guardar datos en el datalake/raw (para batch)
raw_query = final_df.writeStream \
    .format("parquet") \
    .option("path", RAW_PATH) \
    .option("checkpointLocation", CHECKPOINT_LOCATION + "/raw") \
    .outputMode("append") \
    .start()

# Mostrar datos en consola (para speed)
console_query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", CHECKPOINT_LOCATION + "/console") \
    .start()

# Función para escribir en InfluxDB
def write_to_influx(batch_df, batch_id):
    try:
        row_count = batch_df.count()
        if row_count == 0:
            return
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        from influxdb_client.client.write_api import SYNCHRONOUS
        write_api = client.write_api(write_options=SYNCHRONOUS)
        rows = batch_df.collect()
        for row in rows:
            event_type = row['event_type'] if row['event_type'] is not None else 'unknown'
            product_id = row['product_id'] if row['product_id'] is not None else 'none'
            quantity = float(row['quantity']) if row['quantity'] is not None else 0.0
            total = float(row['total']) if row['total'] is not None else 0.0
            event_time = row['event_time'] if row['event_time'] is not None else datetime.utcnow()
            point = (
                Point("sales")
                .tag("event_type", str(event_type))
                .tag("product_id", str(product_id))
                .field("quantity", quantity)
                .field("total", total)
                .time(event_time, WritePrecision.NS)
            )
            try:
                write_api.write(bucket=INFLUX_BUCKET, record=point)
            except Exception as e:
                pass
        client.close()
    except Exception:
        pass

# Escribir en InfluxDB (foreachBatch)
influx_query = final_df.writeStream \
    .foreachBatch(write_to_influx) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_LOCATION + "/influx") \
    .start()

raw_query.awaitTermination()
console_query.awaitTermination()
influx_query.awaitTermination()
