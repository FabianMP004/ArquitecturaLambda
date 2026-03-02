from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType

# 1) Spark session
spark = SparkSession.builder \
    .appName("Ecommerce Batch Processing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2) Schema (lo que produce tu producer.py)
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

# 3) Read from Kafka (STREAM)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "log_de_eventos") \
    .option("startingOffsets", "earliest") \
    .load()

# 4) Parse Kafka value (bytes) -> string -> json -> columns
json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")

parsed_df = json_df.select(
    from_json(col("json_value"), schema).alias("root")
)

# 5) Flatten (root.data.*)
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

# 6) Output to console (STREAM)
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "./checkpoints/console") \
    .start()

query.awaitTermination()