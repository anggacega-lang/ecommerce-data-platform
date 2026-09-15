import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType
)

spark = (
    SparkSession.builder
    .appName("BronzeCustomersStreaming")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField(
        "before",
        StructType([
            StructField("customer_id", IntegerType()),
            StructField("first_name", StringType()),
            StructField("last_name", StringType()),
            StructField("email", StringType()),
            StructField("city", StringType()),
            StructField("country", StringType()),
            StructField("created_at", LongType()),
            StructField("updated_at", LongType())
        ])
    ),
    StructField(
        "after",
        StructType([
            StructField("customer_id", IntegerType()),
            StructField("first_name", StringType()),
            StructField("last_name", StringType()),
            StructField("email", StringType()),
            StructField("city", StringType()),
            StructField("country", StringType()),
            StructField("created_at", LongType()),
            StructField("updated_at", LongType())
        ])
    ),
    StructField("op", StringType()),
    StructField("ts_ms", LongType())
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ecommerce.public.customers")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("data"))
    .select("data.*")
)

bronze_stream = (
    parsed_stream
    .select(
        col("before"),
        col("after"),
        col("op"),
        col("ts_ms")
    )
)

query = (
    bronze_stream.writeStream
    .format("parquet")
    .option(
        "path",
        "s3a://bronze/customers/"
    )
    .option(
        "checkpointLocation",
        "/opt/spark/checkpoints/bronze_customers"
    )
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
