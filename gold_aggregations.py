import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("Fintech-Gold-Aggregations")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_path = "s3a://lakehouse/silver/transactions"
gold_path = "s3a://lakehouse/gold/merchant_daily_summary"

print("Reading verified records from Silver Delta layer...")
silver_df = spark.read.format("delta").load(silver_path)

clean_df = (
    silver_df
    .filter((col("is_deleted") == False) | (col("is_deleted").isNull()))
    .withColumn("transaction_date", to_date(col("created_at")))
)

gold_df = (
    clean_df.groupBy("merchant_id", "transaction_date", "currency").agg(
        F.count("*").alias("total_transactions"),
        F.count(F.when(col("status") == "SETTLED", 1)).alias("settled_count"),
        F.count(F.when(col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.round(F.coalesce(F.sum(F.when(col("status") == "SETTLED", col("amount"))), F.lit(0.0)), 2).alias("total_settled_volume"),
        F.round(F.coalesce(F.sum(F.when(col("status") == "REFUNDED", col("amount"))), F.lit(0.0)), 2).alias("total_refunded_volume")
    )
    .withColumn("net_revenue", F.round(col("total_settled_volume") - col("total_refunded_volume"), 2))
)

print(f"Writing Gold aggregated metrics to {gold_path}...")
(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("transaction_date")
    .save(gold_path)
)

print("Gold write complete. Reading snapshot from Gold Delta table:")
spark.read.format("delta").load(gold_path).show(truncate=False)

spark.stop()