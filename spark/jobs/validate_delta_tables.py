import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ValidateSilverGoldReadOnly")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key",
            os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key",
            os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

tables = [
    ("customers", "s3a://silver/customers"),
    ("products", "s3a://silver/products"),
    ("inventory", "s3a://silver/inventory"),
    ("order_items", "s3a://silver/order_items"),
    ("orders", "s3a://silver/orders"),
    ("payments", "s3a://silver/payments"),
    ("gold_order_summary", "s3a://gold/order_summary"),
]

try:
    for name, path in tables:
        print(f"\n===== {name} =====")
        try:
            df = spark.read.format("delta").load(path)
            print("Columns:", df.columns)
            print("Row count:", df.count())
            df.show(5, truncate=False)
        except Exception as exc:
            print(f"VALIDATION ERROR for {name}: {exc}")

finally:
    spark.stop()
