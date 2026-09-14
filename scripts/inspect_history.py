from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("InspectHistory").getOrCreate()

target_path = "s3a://lakehouse/silver/transactions"
dt = DeltaTable.forPath(spark, target_path)

print("\n=== DELTA TRANSACTION LOG HISTORY ===")
dt.history().select("version", "timestamp", "operation", "operationMetrics").show(5, truncate=False)