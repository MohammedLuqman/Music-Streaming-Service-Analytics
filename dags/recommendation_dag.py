from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from recommendation_jobs import generate_and_push_recommendations

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'music_realtime_recommendations_ai',
    default_args=default_args,
    description='AI Recommendation Engine',
    schedule_interval='0 * * * *', 
    catchup=False,
    max_active_runs=1
) as dag:

    ai_recommendation_task = PythonOperator(
        task_id='generate_user_recommendations',
        python_callable=generate_and_push_recommendations,
    )