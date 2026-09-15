import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, get_json_object
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

spark = (
    SparkSession.builder
    .appName("CustomersBronzeStreaming")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True),
])

debezium_schema = StructType([
    StructField("before", customer_schema, True),
    StructField("after", customer_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ecommerce.public.customers")
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
        from_json(col("payload_json"), debezium_schema)
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

query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "s3a://bronze/customers/")
    .option("checkpointLocation", "/opt/spark/checkpoints/customers_bronze")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
