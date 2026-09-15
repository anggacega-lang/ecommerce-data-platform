import os
from decimal import Decimal

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    from_unixtime,
    udf,
)
from pyspark.sql.types import DecimalType
import base64


# ============================================================
# SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("Gold Order Summary")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# ============================================================
# S3A / MINIO CONFIG
# ============================================================

spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.endpoint",
    "http://minio:9000"
)

spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.access.key",
    os.environ["MINIO_ROOT_USER"]
)

spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.secret.key",
    os.environ["MINIO_ROOT_PASSWORD"]
)

spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.path.style.access",
    "true"
)

spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.connection.ssl.enabled",
    "false"
)

spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

spark.sparkContext._jsc.hadoopConfiguration().set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)


# ============================================================
# DECIMAL DECODER
# Kafka Connect Decimal -> Base64 -> Decimal
# ============================================================

def decode_decimal(value):
    if value is None:
        return None

    try:
        raw = base64.b64decode(value)
        unscaled = int.from_bytes(
            raw,
            byteorder="big",
            signed=True
        )

        return Decimal(unscaled) / Decimal(100)

    except Exception:
        return None


decode_decimal_udf = udf(
    decode_decimal,
    DecimalType(15, 2)
)


# ============================================================
# READ SILVER
# ============================================================

silver_base = "s3a://silver"

orders = (
    spark.read
    .format("delta")
    .load(f"{silver_base}/orders")
)

customers = (
    spark.read
    .format("delta")
    .load(f"{silver_base}/customers")
)

payments = (
    spark.read
    .format("delta")
    .load(f"{silver_base}/payments")
)

order_items = (
    spark.read
    .format("delta")
    .load(f"{silver_base}/order_items")
)


# ============================================================
# TRANSFORM ORDERS
# ============================================================

orders_transformed = (
    orders
    .withColumn(
        "order_date",
        (col("order_date") / 1000000).cast("timestamp")
    )
    .withColumn(
        "updated_at",
        (col("updated_at") / 1000000).cast("timestamp")
    )
    .withColumn(
        "total_amount",
        decode_decimal_udf(col("total_amount"))
    )
)


# ============================================================
# TRANSFORM PAYMENTS
# ============================================================

payments_transformed = (
    payments
    .withColumn(
        "amount",
        decode_decimal_udf(col("amount"))
    )
    .withColumn(
        "paid_at",
        (col("paid_at") / 1000000).cast("timestamp")
    )
    .select(
        "order_id",
        "payment_method",
        "payment_status",
        "amount",
        "paid_at"
    )
)


# ============================================================
# AGGREGATE ORDER ITEMS
# ============================================================

order_items_agg = (
    order_items
    .groupBy("order_id")
    .agg(
        count("order_item_id").alias("item_count"),
        countDistinct("product_id").alias("unique_product_count")
    )
)


# ============================================================
# BUILD GOLD ORDER SUMMARY
# ============================================================

gold_orders = (
    orders_transformed.alias("o")

    .join(
        customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )

    .join(
        payments_transformed.alias("p"),
        col("o.order_id") == col("p.order_id"),
        "left"
    )

    .join(
        order_items_agg.alias("oi"),
        col("o.order_id") == col("oi.order_id"),
        "left"
    )

    .select(
        col("o.order_id"),
        col("o.customer_id"),

        col("c.first_name").alias("customer_first_name"),
        col("c.last_name").alias("customer_last_name"),
        col("c.email").alias("customer_email"),
        col("c.city").alias("customer_city"),
        col("c.country").alias("customer_country"),

        col("o.order_date"),
        col("o.status").alias("order_status"),
        col("o.total_amount"),

        col("p.payment_method"),
        col("p.payment_status"),
        col("p.amount").alias("payment_amount"),
        col("p.paid_at"),

        col("oi.item_count"),
        col("oi.unique_product_count"),

        col("o.updated_at")
    )
)


# ============================================================
# WRITE GOLD
# ============================================================

gold_path = "s3a://gold/order_summary"

(
    gold_orders
    .write
    .format("delta")
    .mode("overwrite")
    .save(gold_path)
)


# ============================================================
# VALIDATION
# ============================================================

print("========== GOLD ORDER SUMMARY ==========")

gold_orders.printSchema()

gold_orders.show(
    truncate=False
)

print(f"Gold records: {gold_orders.count()}")

print("=========================================")

spark.stop()
