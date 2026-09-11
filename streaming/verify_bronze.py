from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verify-Bronze")
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

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("s3a://lakehouse/bronze/transactions")
print("\n--- SCHEMA ---")
df.printSchema()

print("\n--- DATA ROWS ---")
df.select("cdc_operation", "after_image.transaction_id", "after_image.amount", "after_image.status", "ingest_date").show(truncate=False)