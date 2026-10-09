from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

spark = (
    SparkSession.builder
    .appName("TestKafkaRead")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ecommerce.public.customers")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

result = (
    df.select(
        col("partition"),
        col("offset"),
        get_json_object(
            col("value").cast("string"), "$.payload.op"
        ).alias("operation"),
        get_json_object(
            col("value").cast("string"), "$.payload.after.customer_id"
        ).alias("customer_id"),
        get_json_object(
            col("value").cast("string"), "$.payload.after.city"
        ).alias("city")
    )
)

result.show(10, truncate=False)
print("Total records read:", result.count())

spark.stop()
