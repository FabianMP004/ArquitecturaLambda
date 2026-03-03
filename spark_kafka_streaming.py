# spark_kafka_streaming.py
# Unifica el procesamiento batch y speed en un solo Spark Streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType
import os
 
# Configuración
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "log_de_eventos"
CHECKPOINT_LOCATION = "./checkpoints/streaming"
RAW_PATH = "./datalake/raw/events"

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

raw_query.awaitTermination()
console_query.awaitTermination()
