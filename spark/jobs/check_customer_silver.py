from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("CheckCustomerSilver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", __import__("os").environ["MINIO_ROOT_USER"])
hadoop_conf.set("fs.s3a.secret.key", __import__("os").environ["MINIO_ROOT_PASSWORD"])
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark.read.format("delta").load("s3a://silver/customers") \
    .filter("customer_id = 9") \
    .select("customer_id", "first_name", "city", "event_timestamp") \
    .show(truncate=False)

spark.stop()
