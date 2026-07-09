from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def check_streaming_query_status():
    spark_api_url = "http://pyspark-notebook:4040/api/v1/applications"
    
    try:
        response = requests.get(spark_api_url, timeout=5)
        if response.status_code == 200:
            apps = response.json()
            if not apps:
                raise Exception("NO spark app is ON")
            
            print(" Spark Driver is alive and responding.")
            return True
        else:
            raise Exception(f" Spark API : {response.status_code}")
            
    except Exception as e:
        raise Exception(f"{str(e)}")

dag = DAG(
    'streaming_health_check',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False
)

PythonOperator(task_id='check_streaming_status', python_callable=check_streaming_query_status, dag=dag)