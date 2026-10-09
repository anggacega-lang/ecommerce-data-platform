import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TestMinIOWrite")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
hadoop_conf.set("fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

df = spark.createDataFrame(
    [
        (1, "MinIO connection test"),
        (2, "PySpark write test"),
    ],
    ["id", "message"]
)

target = "s3a://bronze/_connection_test/"

df.write.mode("overwrite").parquet(target)

print("SUCCESS: PySpark wrote Parquet to", target)

spark.stop()
