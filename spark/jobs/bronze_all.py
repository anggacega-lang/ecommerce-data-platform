import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, get_json_object
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType


spark = (
    SparkSession.builder
    .appName("EcommerceBronzeStreaming")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# SCHEMAS
# ============================================================

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True),
])

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),
    StructField("stock", IntegerType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True),
])

inventory_schema = StructType([
    StructField("inventory_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("updated_at", LongType(), True),
])

order_items_schema = StructType([
    StructField("order_item_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", StringType(), True),
    StructField("subtotal", StringType(), True),
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", LongType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("updated_at", LongType(), True),
])

payments_schema = StructType([
    StructField("payment_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("paid_at", LongType(), True),
])


# ============================================================
# DEBEZIUM SCHEMA
# ============================================================

def create_debezium_schema(row_schema):
    return StructType([
        StructField("before", row_schema, True),
        StructField("after", row_schema, True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
    ])


# ============================================================
# TABLE CONFIGURATION
# ============================================================

tables = {
    "customers": {
        "topic": "ecommerce.public.customers",
        "schema": customers_schema,
    },
    "products": {
        "topic": "ecommerce.public.products",
        "schema": products_schema,
    },
    "inventory": {
        "topic": "ecommerce.public.inventory",
        "schema": inventory_schema,
    },
    "order_items": {
        "topic": "ecommerce.public.order_items",
        "schema": order_items_schema,
    },
    "orders": {
        "topic": "ecommerce.public.orders",
        "schema": orders_schema,
    },
    "payments": {
        "topic": "ecommerce.public.payments",
        "schema": payments_schema,
    },
}


# ============================================================
# CREATE STREAM FOR EACH TABLE
# ============================================================

for table_name, config in tables.items():

    topic = config["topic"]
    payload_schema = create_debezium_schema(config["schema"])

    print(f"Starting Bronze stream: {table_name}")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS json_value",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_timestamp"
        )
        .withColumn(
            "payload_json",
            get_json_object(col("json_value"), "$.payload")
        )
        .withColumn(
            "payload",
            from_json(col("payload_json"), payload_schema)
        )
    )

    bronze_df = (
        parsed_df
        .select(
            col("kafka_key"),
            col("payload.before").alias("before"),
            col("payload.after").alias("after"),
            col("payload.op").alias("operation"),
            col("payload.ts_ms").alias("event_timestamp"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            current_timestamp().alias("processed_at")
        )
    )

    (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "path",
            f"s3a://bronze/{table_name}/"
        )
        .option(
            "checkpointLocation",
            f"/opt/spark/checkpoints/bronze_{table_name}"
        )
        .trigger(processingTime="10 seconds")
        .start()
    )


spark.streams.awaitAnyTermination()
