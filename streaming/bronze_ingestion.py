import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, date_format
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

# 1. Initialize Spark Session with correct MinIO credentials
spark = (
    SparkSession.builder
    .appName("Fintech-Bronze-CDC-Ingestion")
    .master("local[*]")
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

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "admin")
hadoop_conf.set("fs.s3a.secret.key", "password123")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# 2. Define Schema for Debezium JSON Envelope
payload_schema = StructType([
    StructField("before", StructType([
        StructField("transaction_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]), True),
    StructField("after", StructType([
        StructField("transaction_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

envelope_schema = StructType([
    StructField("payload", payload_schema, True)
])

# 3. Read Stream from Kafka
kafka_raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "fintech.public.transactions")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Parse JSON Envelope & Append Ingestion Metadata
parsed_df = (
    kafka_raw_df
    .selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS raw_json")
    .withColumn("data", from_json(col("raw_json"), envelope_schema))
    .select(
        col("kafka_key"),
        col("data.payload.op").alias("cdc_operation"),
        col("data.payload.ts_ms").alias("source_ts_ms"),
        col("data.payload.before").alias("before_image"),
        col("data.payload.after").alias("after_image"),
        col("raw_json"),
        current_timestamp().alias("ingested_at"),
        date_format(current_timestamp(), "yyyy-MM-dd").alias("ingest_date")
    )
)

# 5. Stream Parquet writes into MinIO Bronze Layer
query = (
    parsed_df.writeStream
    .format("parquet")
    .outputMode("append")
    .partitionBy("ingest_date")
    .option("path", "s3a://lakehouse/bronze/transactions")
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze/transactions")
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()