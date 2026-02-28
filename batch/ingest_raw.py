from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType

spark = SparkSession.builder.appName("Batch-Ingest-RAW").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "log_de_eventos") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")

parsed = json_df.select(from_json(col("json_value"), schema).alias("root"))

events = parsed.select(
    col("root.id").alias("id"),
    col("root.timestamp").alias("timestamp"),
    col("root.data.event_type").alias("event_type"),
    col("root.data.user_id").alias("user_id"),
    col("root.data.product_id").alias("product_id"),
    col("root.data.quantity").alias("quantity"),
    col("root.data.items").alias("items"),
    col("root.data.total").alias("total")
)

# Guarda en RAW como Parquet particionado por micro-batch
query = events.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "datalake/raw/events") \
    .option("checkpointLocation", "checkpoints/raw_events") \
    .start()

query.awaitTermination()