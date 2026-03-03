# Este archivo ha sido deshabilitado. Usa spark_kafka_streaming.py
import logging
import sys
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.types import StructField as SStructField
from pyspark.sql.types import StructType as SStructType

# -------------------------------------------------
# CONFIGURATION
# -------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "log_de_eventos"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "2jnFpAv9uNq9Z_3ynatuvQqFdGmmuSx4ecmVouwGXg8Q-S4BaMZm4ZJQFTJ3RsgOFLYIRovw_26Sc4eTCO080A=="
INFLUX_ORG = "lambda_org"
INFLUX_BUCKET = "lambda_speed"

CHECKPOINT_LOCATION = "./checkpoints/speed"

# -------------------------------------------------
# LOGGING
# -------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("spark_kafka_speed")

# -------------------------------------------------
# JSON SCHEMA
# -------------------------------------------------

item_struct = SStructType(
    [
        SStructField("product_id", IntegerType(), True),
        SStructField("quantity", IntegerType(), True),
    ]
)

data_struct = SStructType(
    [
        SStructField("event_type", StringType(), True),
        SStructField("user_id", IntegerType(), True),
        SStructField("product_id", IntegerType(), True),
        SStructField("quantity", IntegerType(), True),
        SStructField("total", FloatType(), True),
        SStructField("items", ArrayType(item_struct), True),
    ]
)

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("data", data_struct, True),
    ]
)

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def write_to_influx(batch_df, batch_id):
    try:
        row_count = batch_df.count()
        if row_count == 0:
            logger.debug("Batch %s has 0 rows, skipping write.", batch_id)
            return

        logger.info("Processing batch %s with %d rows", batch_id, row_count)

        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        
        from influxdb_client.client.write_api import SYNCHRONOUS
        write_api = client.write_api(write_options=SYNCHRONOUS)

        rows = batch_df.collect()
        for row in rows:
            event_type = (
                row["event_type"]
                if "event_type" in row and row["event_type"] is not None
                else "unknown"
            )
            product_id = (
                row["product_id"]
                if "product_id" in row and row["product_id"] is not None
                else "none"
            )
            quantity = (
                float(row["quantity"])
                if "quantity" in row and row["quantity"] is not None
                else 0.0
            )
            total = (
                float(row["total"])
                if "total" in row and row["total"] is not None
                else 0.0
            )
            event_time = (
                row["event_time"]
                if "event_time" in row and row["event_time"] is not None
                else datetime.utcnow()
            )

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
                logger.exception("Failed to write point for row %s: %s", row, e)

        client.close()
        logger.info("Batch %s written to InfluxDB", batch_id)
    except Exception:
        logger.exception("Exception in write_to_influx for batch %s", batch_id)


def main():
    logger.info("Starting Spark session")
    spark = SparkSession.builder.appName("LambdaSpeedLayerInflux").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "Configuring readStream from Kafka: %s -> %s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
    )
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option(
            "failOnDataLoss", "false"
        )
        .load()
    )

    logger.info("Kafka readStream schema: %s", kafka_df.schema.simpleString())

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string")
    parsed_df = json_df.select(from_json(col("json_string"), schema).alias("root"))

    flat_df = parsed_df.select(
        col("root.id").alias("id"),
        col("root.timestamp").alias("timestamp"),
        col("root.data.event_type").alias("event_type"),
        col("root.data.user_id").alias("user_id"),
        col("root.data.product_id").alias("product_id"),
        col("root.data.quantity").alias("quantity"),
        col("root.data.total").alias("total"),
    )

    final_df = flat_df.withColumn("event_time", to_timestamp(col("timestamp")))

    logger.info(
        "Setting up foreachBatch Influx writer (checkpoint=%s)", CHECKPOINT_LOCATION
    )
    influx_query = (
        final_df.writeStream.foreachBatch(write_to_influx)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    logger.info("Starting console debug sink for visibility")
    debug_query = (
        final_df.writeStream.format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .start()
    )

    logger.info(
        "Streaming started. Influx query id=%s | Debug query id=%s",
        influx_query.id,
        debug_query.id,
    )

    try:
        influx_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, stopping streams...")
    except Exception:
        logger.exception("Unhandled exception when awaiting termination")
    finally:
        logger.info("Stopping queries gracefully")
        try:
            influx_query.stop()
        except Exception:
            logger.debug("influx_query may already be stopped")
        try:
            debug_query.stop()
        except Exception:
            logger.debug("debug_query may already be stopped")
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
