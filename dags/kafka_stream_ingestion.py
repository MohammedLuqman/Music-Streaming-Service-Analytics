from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 6, 26),
    'retries': 999,               
    'retry_delay': timedelta(seconds=5), 
    'retry_exponential_backoff': False, 
}

with DAG(
    dag_id='kafka_stream_ingestion',
    default_args=default_args,
    schedule_interval='*/1 * * * *', 
    catchup=False,
    tags=['streaming', 'kafka', 'continuous']
) as dag:

    task_run_simulator = BashOperator(
        task_id='stream_events_to_kafka',
        bash_command=(
            "python -c 'import kafka' 2>/dev/null || "
            "pip install kafka-python boto3 pandas; "
            "python /opt/airflow/dags/scripts/kafka_bounded_producer.py"
        )
    )

    task_run_simulator