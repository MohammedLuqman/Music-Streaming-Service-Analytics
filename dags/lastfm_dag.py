from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from lastfm_jobs import (
    initialize_minio_bucket, 
    fetch_and_upload_songs_to_minio, 
    generate_and_upload_users_to_minio
)

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 6, 26),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='lastfm_to_minio',
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False,
    tags=['lakehouse', 'minio', 'bronze_layer']
) as dag:

    task_init_lake = PythonOperator(
        task_id='initialize_minio_bucket',
        python_callable=initialize_minio_bucket
    )

    task_songs_to_minio = PythonOperator(
        task_id='fetch_and_upload_songs_to_minio',  
        python_callable=fetch_and_upload_songs_to_minio
    )

    task_users_to_minio = PythonOperator(
        task_id='generate_and_upload_users_to_minio',
        python_callable=generate_and_upload_users_to_minio
    )

    task_init_lake >> [task_songs_to_minio, task_users_to_minio]