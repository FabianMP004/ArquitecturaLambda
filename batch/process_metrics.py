from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, date_format, count, desc, sum as _sum, avg, size,
    when, expr
)

spark = SparkSession.builder.appName("Batch-Process-Metrics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# =========================
# 1) Leer RAW
# =========================
df = spark.read.parquet("datalake/raw/events")
df = df.withColumn("ts", to_timestamp(col("timestamp")))
df = df.filter(col("ts").isNotNull())  # por si viene algo raro

# Helpers de tiempo
df = df.withColumn("day", date_format(col("ts"), "yyyy-MM-dd"))
df = df.withColumn("hour", date_format(col("ts"), "yyyy-MM-dd HH:00:00"))

# =========================
# 2) MÉTRICAS
# =========================

# A) Eventos por tipo
events_by_type = df.groupBy("event_type").agg(count("*").alias("total_eventos")) \
    .orderBy(desc("total_eventos"))
events_by_type.write.mode("overwrite").parquet("datalake/processed/events_by_type")

# B) Eventos por día
events_per_day = df.groupBy("day").agg(count("*").alias("eventos_por_dia")).orderBy("day")
events_per_day.write.mode("overwrite").parquet("datalake/processed/events_per_day")

# C) Eventos por hora
events_per_hour = df.groupBy("hour").agg(count("*").alias("eventos_por_hora")).orderBy("hour")
events_per_hour.write.mode("overwrite").parquet("datalake/processed/events_per_hour")

# D) Top productos vistos (total)
top_viewed_products = df.filter(col("event_type") == "view_product") \
    .groupBy("product_id").agg(count("*").alias("views")) \
    .orderBy(desc("views")).limit(20)
top_viewed_products.write.mode("overwrite").parquet("datalake/processed/top_viewed_products")

# E) Top productos vistos por hora
top_viewed_per_hour = df.filter(col("event_type") == "view_product") \
    .groupBy("hour", "product_id").agg(count("*").alias("views")) \
    .orderBy(desc("views"))
top_viewed_per_hour.write.mode("overwrite").parquet("datalake/processed/top_viewed_per_hour")

# F) Compras: explode items para métricas por producto
purchases = df.filter(col("event_type") == "compra")

items_exploded = purchases.select(
    col("id").alias("order_id"),
    col("day"), col("hour"),
    col("user_id"),
    col("total").alias("order_total"),
    expr("explode(items) as item")
).select(
    "order_id", "day", "hour", "user_id", "order_total",
    col("item.product_id").alias("product_id"),
    col("item.quantity").alias("quantity")
)

# G) Top productos comprados por cantidad
top_products_by_qty = items_exploded.groupBy("product_id") \
    .agg(_sum("quantity").alias("total_qty")) \
    .orderBy(desc("total_qty")).limit(20)
top_products_by_qty.write.mode("overwrite").parquet("datalake/processed/top_products_by_qty")

# H) Revenue por día (suma de total en compras)
revenue_per_day = purchases.groupBy("day") \
    .agg(_sum("total").alias("revenue_dia"), count("*").alias("num_orders")) \
    .orderBy("day")
revenue_per_day.write.mode("overwrite").parquet("datalake/processed/revenue_per_day")

# I) Revenue por hora
revenue_per_hour = purchases.groupBy("hour") \
    .agg(_sum("total").alias("revenue_hora"), count("*").alias("num_orders")) \
    .orderBy("hour")
revenue_per_hour.write.mode("overwrite").parquet("datalake/processed/revenue_per_hour")

# J) AOV (Average Order Value)
aov = purchases.agg(avg("total").alias("avg_order_value"))
aov.write.mode("overwrite").parquet("datalake/processed/aov")

# K) Carrito promedio (items por compra)
cart_size = purchases.withColumn("items_count", size(col("items"))) \
    .agg(avg("items_count").alias("avg_items_per_order"))
cart_size.write.mode("overwrite").parquet("datalake/processed/avg_items_per_order")

# L) Usuarios más activos (por cantidad de eventos)
top_users = df.groupBy("user_id").agg(count("*").alias("total_eventos")) \
    .orderBy(desc("total_eventos")).limit(20)
top_users.write.mode("overwrite").parquet("datalake/processed/top_users")

# M) Funnel simple: view -> add_to_cart -> compra (conteos y ratios)
# Definimos add_to_cart como agregar_carrito + modificar_carrito (puedes ajustarlo)
views = df.filter(col("event_type") == "view_product").count()
adds = df.filter(col("event_type").isin("agregar_carrito", "modificar_carrito")).count()
buys = purchases.count()

funnel_rows = [
    ("views", views),
    ("adds", adds),
    ("buys", buys),
]
funnel_df = spark.createDataFrame(funnel_rows, ["stage", "count"])

# ratios (evitar división por 0)
view_to_add = (adds / views) if views else 0.0
add_to_buy = (buys / adds) if adds else 0.0
view_to_buy = (buys / views) if views else 0.0

ratios_rows = [
    ("view_to_add", float(view_to_add)),
    ("add_to_buy", float(add_to_buy)),
    ("view_to_buy", float(view_to_buy)),
]
ratios_df = spark.createDataFrame(ratios_rows, ["ratio", "value"])

funnel_df.write.mode("overwrite").parquet("datalake/processed/funnel_counts")
ratios_df.write.mode("overwrite").parquet("datalake/processed/funnel_ratios")

print("✅ Métricas batch generadas en datalake/processed/")