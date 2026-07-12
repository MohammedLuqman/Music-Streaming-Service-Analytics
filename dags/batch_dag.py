from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'silver_layer_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    submit_job = SparkSubmitOperator(
        application="/opt/airflow/scripts/batch_job.py",  
        task_id="run_spark_silver_process",
        conn_id="spark_default",  
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        },
        jars="/home/jovyan/jars/*"
    )