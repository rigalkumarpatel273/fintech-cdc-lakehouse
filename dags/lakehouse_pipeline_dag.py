from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "fintech_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-end orchestration for Silver upsert, Gold aggregations, and quality audits",
    schedule_interval="*/10 * * * *",  # Runs every 10 minutes
    catchup=False,
    max_active_runs=1,
)

# 1. Healthcheck: Ensure Debezium connector is healthy
def check_debezium_status():
    # Update hostname from 'fintech_connect' to 'debezium'
    url = "http://debezium:8083/connectors/fintech-postgres-connector/status"
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Debezium connector not reachable: {resp.status_code}")
    state = resp.json().get("tasks", [{}])[0].get("state", "UNKNOWN")
    if state != "RUNNING":
        raise RuntimeError(f"Debezium task state is {state}, expected RUNNING")
    print("Debezium connector is healthy and RUNNING.")

task_check_cdc = PythonOperator(
    task_id="check_cdc_connector_health",
    python_callable=check_debezium_status,
    dag=dag,
)

# 2. Run Silver Upsert inside fintech_spark container
spark_submit_base = (
    "docker exec fintech_spark /opt/spark/bin/spark-submit "
    "--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=admin "
    "--conf spark.hadoop.fs.s3a.secret.key=password123 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
)

task_run_silver = BashOperator(
    task_id="run_silver_upsert",
    bash_command=f"{spark_submit_base} streaming/silver_upsert.py",
    dag=dag,
)

# 3. Run Gold Aggregations
task_run_gold = BashOperator(
    task_id="run_gold_aggregations",
    bash_command=f"{spark_submit_base} streaming/gold_aggregations.py",
    dag=dag,
)

# Pipeline dependency graph
task_check_cdc >> task_run_silver >> task_run_gold