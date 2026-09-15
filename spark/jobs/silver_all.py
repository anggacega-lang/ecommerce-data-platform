import os

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import (
    col,
    lit,
    to_json,
    from_json,
    row_number,
    coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType,
    TimestampType
)
from delta.tables import DeltaTable


builder = (
    SparkSession.builder
    .appName("EcommerceSilverStreaming")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.environ["MINIO_ROOT_USER"]
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ["MINIO_ROOT_PASSWORD"]
    )
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
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# TABLE SCHEMAS
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


tables = {
    "customers": {
        "schema": customers_schema,
        "key": "customer_id"
    },
    "products": {
        "schema": products_schema,
        "key": "product_id"
    },
    "inventory": {
        "schema": inventory_schema,
        "key": "inventory_id"
    },
    "order_items": {
        "schema": order_items_schema,
        "key": "order_item_id"
    },
    "orders": {
        "schema": orders_schema,
        "key": "order_id"
    },
    "payments": {
        "schema": payments_schema,
        "key": "payment_id"
    },
}


# ============================================================
# BRONZE SCHEMA
# ============================================================

def bronze_schema(row_schema):
    return StructType([
        StructField("kafka_key", StringType(), True),
        StructField("before", row_schema, True),
        StructField("after", row_schema, True),
        StructField("operation", StringType(), True),
        StructField("event_timestamp", LongType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("processed_at", TimestampType(), True),
    ])


# ============================================================
# READ ALL BRONZE STREAMS
# ============================================================

streams = []

for table_name, config in tables.items():

    df = (
        spark.readStream
        .format("parquet")
        .schema(bronze_schema(config["schema"]))
        .load(f"s3a://bronze/{table_name}")
        .select(
            lit(table_name).alias("table_name"),
            to_json(col("before")).alias("before_json"),
            to_json(col("after")).alias("after_json"),
            col("operation"),
            col("event_timestamp"),
            col("offset")
        )
    )

    streams.append(df)


bronze_stream = streams[0]

for df in streams[1:]:
    bronze_stream = bronze_stream.unionByName(df)


# ============================================================
# FOREACH BATCH
# ============================================================

def process_batch(batch_df, batch_id):

    print(f"\n========== SILVER BATCH {batch_id} ==========")

    if batch_df.rdd.isEmpty():
        print("Empty batch")
        return

    for table_name, config in tables.items():

        table_df = batch_df.filter(
            col("table_name") == table_name
        )

        if table_df.rdd.isEmpty():
            continue

        schema = config["schema"]
        key = config["key"]

        # ----------------------------------------------------
        # Parse Debezium before / after JSON
        # ----------------------------------------------------

        parsed_df = (
            table_df
            .withColumn(
                "before",
                from_json(col("before_json"), schema)
            )
            .withColumn(
                "after",
                from_json(col("after_json"), schema)
            )
        )

        # ----------------------------------------------------
        # Determine primary key
        # DELETE uses before
        # INSERT/UPDATE uses after
        # ----------------------------------------------------

        events_df = parsed_df.withColumn(
            "record_key",
            coalesce(
                col(f"after.{key}"),
                col(f"before.{key}")
            )
        )

        # ----------------------------------------------------
        # Keep latest event per primary key
        # ----------------------------------------------------

        window_spec = (
            Window
            .partitionBy("record_key")
            .orderBy(
                col("event_timestamp").desc_nulls_last(),
                col("offset").desc()
            )
        )

        latest_df = (
            events_df
            .withColumn(
                "_rn",
                row_number().over(window_spec)
            )
            .filter(col("_rn") == 1)
            .drop("_rn")
        )

        target_path = f"s3a://silver/{table_name}"

        # ----------------------------------------------------
        # Create empty Delta table if it doesn't exist
        # ----------------------------------------------------

        if not DeltaTable.isDeltaTable(
            spark,
            target_path
        ):

            (
                latest_df
                .filter(col("operation") != "d")
                .select("after.*")
                .limit(0)
                .write
                .format("delta")
                .mode("overwrite")
                .save(target_path)
            )

            print(f"{table_name}: Delta table created")

        delta_table = DeltaTable.forPath(
            spark,
            target_path
        )

        # ====================================================
        # DELETE
        # ====================================================

        delete_df = (
            latest_df
            .filter(col("operation") == "d")
            .select(
                col("record_key").alias(key)
            )
            .filter(col(key).isNotNull())
        )

        if not delete_df.rdd.isEmpty():

            (
                delta_table.alias("target")
                .merge(
                    delete_df.alias("source"),
                    f"target.{key} = source.{key}"
                )
                .whenMatchedDelete()
                .execute()
            )

            print(
                f"{table_name}: DELETE "
                f"{delete_df.count()}"
            )

        # ====================================================
        # UPSERT
        # ====================================================

        upsert_df = (
            latest_df
            .filter(
                col("operation").isin(
                    "c",
                    "u",
                    "r"
                )
            )
            .select("after.*")
        )

        if not upsert_df.rdd.isEmpty():

            target_columns = [
                field.name
                for field in schema.fields
            ]

            update_map = {
                column_name:
                    f"source.{column_name}"
                for column_name in target_columns
            }

            insert_map = {
                column_name:
                    f"source.{column_name}"
                for column_name in target_columns
            }

            (
                delta_table.alias("target")
                .merge(
                    upsert_df.alias("source"),
                    f"target.{key} = source.{key}"
                )
                .whenMatchedUpdate(
                    set=update_map
                )
                .whenNotMatchedInsert(
                    values=insert_map
                )
                .execute()
            )

            print(
                f"{table_name}: UPSERT "
                f"{upsert_df.count()}"
            )


# ============================================================
# START STREAM
# ============================================================

query = (
    bronze_stream
    .writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        "/opt/spark/checkpoints/silver_all"
    )
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()

