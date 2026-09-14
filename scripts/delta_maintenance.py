# delta_maintenance.py
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

def run_maintenance():
    spark = SparkSession.builder.appName("DeltaMaintenance").getOrCreate()
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    
    # Target Silver Tables
    target_path = "s3a://lakehouse/silver/transactions"
    dt = DeltaTable.forPath(spark, target_path)
    
    print(f"Compacting {target_path}...")
    dt.optimize().executeCompaction()
    
    print(f"Vacuuming {target_path}...")
    dt.vacuum(0)
    print("Done.")

if __name__ == "__main__":
    run_maintenance()