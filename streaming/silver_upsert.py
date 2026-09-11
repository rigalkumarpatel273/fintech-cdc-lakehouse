from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. Initialize Spark Session configured for Delta Lake + S3A
spark = (
    SparkSession.builder
    .appName("Fintech-Silver-CDC-Merge")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Read Bronze Parquet Data
bronze_df = spark.read.parquet("s3a://lakehouse/bronze/transactions")

# 3. Clean and Unpack Mutations
cleaned_df = bronze_df.select(
    col("cdc_operation"),
    col("source_ts_ms"),
    col("after_image.transaction_id").alias("transaction_id"),
    col("after_image.merchant_id").alias("merchant_id"),
    col("after_image.amount").cast("decimal(18,2)").alias("amount"),
    col("after_image.currency").alias("currency"),
    col("after_image.status").alias("status"),
    to_timestamp(col("after_image.created_at")).alias("created_at"),
    to_timestamp(col("after_image.updated_at")).alias("updated_at"),
    col("ingested_at")
)

# 4. Deduplicate In-Flight Mutations (Keep latest source_ts_ms per transaction_id)
window_spec = Window.partitionBy("transaction_id").orderBy(col("source_ts_ms").desc())
latest_mutations = (
    cleaned_df
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") == 1)
    .drop("rank")
)

silver_path = "s3a://lakehouse/silver/transactions"

# 5. Apply ACID Upsert (Merge) into Delta Lake
if not DeltaTable.isDeltaTable(spark, silver_path):
    print("Initializing Silver Delta Lake table...")
    latest_mutations.write.format("delta").mode("overwrite").save(silver_path)
    print("Silver table initialized.")
else:
    print("Merging mutations into existing Silver Delta table...")
    silver_table = DeltaTable.forPath(spark, silver_path)
    
    (
        silver_table.alias("target")
        .merge(
            latest_mutations.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedUpdateAll(
            condition="source.source_ts_ms > target.source_ts_ms"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Merge complete.")

# 6. Preview Current Silver State
silver_df = spark.read.format("delta").load(silver_path)
print("\n--- SILVER LAYER (CURRENT SNAPSHOT) ---")
silver_df.show(truncate=False)