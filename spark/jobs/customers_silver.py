import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("CustomersSilverStreaming")
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
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Paths
# ============================================================

BRONZE_PATH = "s3a://bronze/customers/"
SILVER_PATH = "s3a://silver/customers/"
CHECKPOINT_PATH = "/opt/spark/checkpoints/customers_silver"


# ============================================================
# Read Bronze CDC as Streaming
# ============================================================

bronze_schema = (
    spark.read
    .format("parquet")
    .load(BRONZE_PATH)
    .schema
)

bronze_df = (
    spark.readStream
    .format("parquet")
    .schema(bronze_schema)
    .load(BRONZE_PATH)
)


# ============================================================
# Process each micro-batch
# ============================================================

def process_batch(batch_df, batch_id):

    print(f"\n===== Processing Silver Batch {batch_id} =====")

    if batch_df.isEmpty():
        print("Batch is empty.")
        return

    # --------------------------------------------------------
    # Convert nested Bronze CDC structure into Silver CDC
    # --------------------------------------------------------

    cdc_df = (
        batch_df
        .select(
            col("operation"),
            col("before.customer_id").alias("before_customer_id"),
            col("after.customer_id").alias("after_customer_id"),
            col("after.first_name").alias("first_name"),
            col("after.last_name").alias("last_name"),
            col("after.email").alias("email"),
            col("after.city").alias("city"),
            col("after.country").alias("country"),
            col("after.created_at").alias("created_at"),
            col("after.updated_at").alias("updated_at"),
            col("event_timestamp"),
            col("offset")
        )
        .withColumn(
            "customer_id",
            col("after_customer_id").alias("customer_id")
        )
    )

    # --------------------------------------------------------
    # For DELETE, customer_id comes from BEFORE
    # For CREATE/UPDATE, customer_id comes from AFTER
    # --------------------------------------------------------

    cdc_df = (
        cdc_df
        .withColumn(
            "customer_id",
            col("after_customer_id")
        )
        .withColumn(
            "customer_id",
            col("customer_id")
        )
    )

    cdc_df = cdc_df.select(
        "operation",
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "country",
        "created_at",
        "updated_at",
        "event_timestamp",
        "offset",
        "before_customer_id"
    )

    # --------------------------------------------------------
    # Fix customer_id for DELETE
    # --------------------------------------------------------

    from pyspark.sql.functions import when

    cdc_df = (
        cdc_df
        .withColumn(
            "customer_id",
            when(
                col("operation") == "d",
                col("before_customer_id")
            ).otherwise(col("customer_id"))
        )
    )

    # --------------------------------------------------------
    # Deduplicate CDC events inside this micro-batch
    #
    # If the same customer appears multiple times in one batch,
    # keep the latest event based on event_timestamp + offset.
    # --------------------------------------------------------

    window_spec = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            col("event_timestamp").desc(),
            col("offset").desc()
        )
    )

    cdc_df = (
        cdc_df
        .withColumn(
            "row_number",
            row_number().over(window_spec)
        )
        .filter(col("row_number") == 1)
        .drop("row_number")
    )

    print("CDC events in current batch:")

    cdc_df.select(
        "operation",
        "customer_id",
        "first_name",
        "last_name",
        "city",
        "event_timestamp",
        "offset"
    ).show(truncate=False)

    # --------------------------------------------------------
    # Separate DELETE and UPSERT events
    # --------------------------------------------------------

    delete_df = (
        cdc_df
        .filter(col("operation") == "d")
        .select("customer_id")
    )

    upsert_df = (
        cdc_df
        .filter(col("operation").isin("c", "u", "r"))
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "country",
            "created_at",
            "updated_at",
            "event_timestamp"
        )
    )

    # --------------------------------------------------------
    # Create Silver Delta table if it doesn't exist
    # --------------------------------------------------------
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):

        print("Silver Delta table does not exist.")
        print("Initializing empty Silver Delta table...")

        (
            upsert_df
            .limit(0)
            .write
            .format("delta")
            .mode("overwrite")
            .save(SILVER_PATH)
        )

        print("Silver Delta table initialized.")

    # --------------------------------------------------------
    # Load Silver Delta table
    # --------------------------------------------------------
    silver_table = DeltaTable.forPath(
        spark,
        SILVER_PATH
    )

    # --------------------------------------------------------
    # UPSERT
    # --------------------------------------------------------
    if not upsert_df.isEmpty():

        print("Applying UPSERT...")

        (
            silver_table.alias("target")
            .merge(
                upsert_df.alias("source"),
                "target.customer_id = source.customer_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("UPSERT completed.")

    # --------------------------------------------------------
    # DELETE
    # --------------------------------------------------------
    if not delete_df.isEmpty():

        print("Applying DELETE...")

        (
            silver_table.alias("target")
            .merge(
                delete_df.alias("source"),
                "target.customer_id = source.customer_id"
            )
            .whenMatchedDelete()
            .execute()
        )

        print("DELETE completed.")

    print(f"===== Silver Batch {batch_id} completed =====\n")

# ============================================================
# Start Streaming Query
# ============================================================

query = (
    bronze_df
    .writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
