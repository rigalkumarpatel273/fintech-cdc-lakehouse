import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when, to_timestamp, current_timestamp, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# 1. Initialize Spark Session with Delta and S3A support
spark = SparkSession.builder \
    .appName("Fintech-Silver-CDC-Merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

bronze_path = "s3a://lakehouse/bronze/transactions"
silver_path = "s3a://lakehouse/silver/transactions"
quarantine_path = "s3a://lakehouse/quarantine/transactions"

print("Reading raw records from Bronze layer...")
bronze_df = spark.read.parquet(bronze_path)

# 2. Extract fields with Deletion Awareness (coalesce after and before images)
cleaned_df = bronze_df.select(
    col("cdc_operation"),
    col("source_ts_ms"),
    
    # Primary Key
    coalesce(
        col("after_image.transaction_id"), 
        col("before_image.transaction_id")
    ).alias("transaction_id"),
    
    # Dimension & Attribute Fields
    coalesce(
        col("after_image.merchant_id"), 
        col("before_image.merchant_id")
    ).alias("merchant_id"),
    
    coalesce(
        col("after_image.amount"), 
        col("before_image.amount")
    ).cast(DecimalType(18, 2)).alias("amount"),
    
    coalesce(
        col("after_image.currency"), 
        col("before_image.currency")
    ).alias("currency"),
    
    coalesce(
        col("after_image.status"), 
        col("before_image.status")
    ).alias("status"),
    
    to_timestamp(
        coalesce(col("after_image.created_at"), col("before_image.created_at"))
    ).alias("created_at"),
    
    to_timestamp(
        coalesce(col("after_image.updated_at"), col("before_image.updated_at"))
    ).alias("updated_at"),
    
    # Soft Delete Flag using cdc_operation
    when(col("cdc_operation") == "d", True).otherwise(False).alias("is_deleted"),
    col("ingested_at")
)

# 3. Data Quality Gate & Quarantine Split
reason_df = cleaned_df.withColumn(
    "rejection_reason",
    when(col("transaction_id").isNull(), "NULL_TRANSACTION_ID")
    .when((col("amount").isNull()) | (col("amount") <= 0), "INVALID_AMOUNT")
    .when(~col("status").isin("SETTLED", "PENDING", "REFUNDED"), "INVALID_STATUS")
    .otherwise(None)
)

valid_df = reason_df.filter(col("rejection_reason").isNull()).drop("rejection_reason")

quarantine_df = reason_df.filter(col("rejection_reason").isNotNull()) \
    .withColumn("quarantined_at", current_timestamp())

# Persist rejected records to Quarantine Delta Table (Append-only)
if quarantine_df.rdd.isEmpty():
    print("No corrupted records detected in this run.")
else:
    print(f"Quarantining corrupted records to {quarantine_path}...")
    quarantine_df.write \
        .format("delta") \
        .mode("append") \
        .save(quarantine_path)

# 4. Deduplicate Valid Records within Incoming Micro-Batch
window_spec = Window.partitionBy("transaction_id").orderBy(col("source_ts_ms").desc())

latest_mutations = valid_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# 5. ACID Upsert into Silver Delta Lake
# 5. ACID Upsert into Silver Delta Lake
if not DeltaTable.isDeltaTable(spark, silver_path):
    print("Silver table does not exist. Initializing new Delta table...")
    latest_mutations.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_path)
    print("Silver table initialized.")

    # Apply Delta invariants immediately after first initialization
    print("Applying Delta invariants (CHECK constraints)...")
    spark.sql(f"ALTER TABLE delta.`{silver_path}` ADD CONSTRAINT valid_amount CHECK (amount > 0)")
    spark.sql(f"ALTER TABLE delta.`{silver_path}` ADD CONSTRAINT valid_transaction_id CHECK (transaction_id IS NOT NULL)")
    print("Constraints applied successfully.")
else:
    print("Merging mutations into existing Silver Delta table...")
    silver_table = DeltaTable.forPath(spark, silver_path)
    
    silver_table.alias("target").merge(
        latest_mutations.alias("source"),
        "target.transaction_id = source.transaction_id"
    ).whenMatchedUpdate(
        condition="source.source_ts_ms >= target.source_ts_ms",
        set={
            "cdc_operation": "source.cdc_operation",
            "source_ts_ms": "source.source_ts_ms",
            "merchant_id": "source.merchant_id",
            "amount": "source.amount",
            "currency": "source.currency",
            "status": "source.status",
            "created_at": "source.created_at",
            "updated_at": "source.updated_at",
            "is_deleted": "source.is_deleted",
            "ingested_at": "source.ingested_at"
        }
    ).whenNotMatchedInsert(
        values={
            "cdc_operation": "source.cdc_operation",
            "source_ts_ms": "source.source_ts_ms",
            "transaction_id": "source.transaction_id",
            "merchant_id": "source.merchant_id",
            "amount": "source.amount",
            "currency": "source.currency",
            "status": "source.status",
            "created_at": "source.created_at",
            "updated_at": "source.updated_at",
            "is_deleted": "source.is_deleted",
            "ingested_at": "source.ingested_at"
        }
    ).execute()
    print("Merge completed successfully.")

# 6. Display Table Snapshots
print("\n--- SILVER LAYER (CLEAN SNAPSHOT) ---")
spark.read.format("delta").load(silver_path).show(truncate=False)

if DeltaTable.isDeltaTable(spark, quarantine_path):
    print("\n--- QUARANTINE LAYER (REJECTED RECORDS) ---")
    spark.read.format("delta").load(quarantine_path).show(truncate=False)

spark.stop()