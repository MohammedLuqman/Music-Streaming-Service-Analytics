from datetime import datetime, timedelta
import random
import requests
import json
import io
import pandas as pd
import boto3
from botocore.client import Config
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

logger = logging.getLogger("airflow.task")

LASTFM_API_KEY = "f99a3068a7212836272c4f98a4b351f9"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "minio_password"
BUCKET_NAME = "landing-zone"

def get_minio_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )

def initialize_minio_bucket():
    s3_client = get_minio_client()
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Bucket '{BUCKET_NAME}' already exists in MinIO.")
    except s3_client.exceptions.ClientError:
        logger.info(f"Bucket '{BUCKET_NAME}' not found. Creating it now")
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        logger.info(f"Bucket '{BUCKET_NAME}' initialized successfully.")

def fetch_and_upload_songs_to_minio():
    logger.info("Extracting tracks from Last.fm API")
    s3_client = get_minio_client()
    
    url = "http://ws.audioscrobbler.com/2.0/"
    genres_pool = ["Pop", "Hip-Hop", "Rock", "R&B", "Electronic", "Alternative", "Indie", "Jazz"]
    raw_songs_list = []
    records_per_page = 100
    total_pages = 50
    
    try:
        for page in range(1, total_pages + 1):
            logger.info(f"Fetching page {page} from Last.fm API")
            params = {
                "method": "chart.gettoptracks",
                "api_key": LASTFM_API_KEY,
                "format": "json",
                "limit": records_per_page,
                "page": page
            }
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"API Failure on page {page}: {response.text}")
                raise Exception(f"Last.fm API connection dropped with status: {response.status_code}")
                
            tracks = response.json()["tracks"]["track"]
            
            for track in tracks:
                if not track.get("name") or not track.get("artist", {}).get("name"):
                    logger.warning("Skipping a corrupted record missing critical name/artist fields.")
                    continue
                
                song_id = track.get("mbid") if track.get("mbid") else f"lf_{abs(hash(track['name'] + track['artist']['name'])) % 100000}"
                
                song_data = {
                    "song_id": song_id,
                    "song_name": track["name"],
                    "artist_name": track["artist"]["name"],
                    "album_name": f"{track['name']} - Single",
                    "duration_ms": random.randint(180000, 300000),
                    "genre": random.choice(genres_pool),
                    "ingested_at": datetime.now().isoformat()
                }
                raw_songs_list.append(song_data)
                
        json_buffer = io.BytesIO(json.dumps(raw_songs_list, ensure_ascii=False, indent=4).encode('utf-8'))
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destination_path = f"bronze/songs/songs_raw_{timestamp}.json"
        
        s3_client.upload_fileobj(json_buffer, BUCKET_NAME, destination_path)
        logger.info(f"Successfully uploaded {len(raw_songs_list)} raw songs to MinIO at '{destination_path}'")
        
    except Exception as e:
        logger.error(f"Failed to execute API extraction: {e}")
        raise e

def generate_and_upload_users_to_minio():
    logger.info("Generating  users profiles")
    s3_client = get_minio_client()
    
    names = ["Ahmed", "Sara", "John", "Emma", "Mohamed", "Fatima", "David", "Olivia", "Ali", "Sophia"]
    countries = ["Egypt", "Yemen", "Saudi Arabia", "USA", "UK", "Canada", "Germany"]
    subs = ["Free", "Premium"]
    
    users_list = []
    for i in range(1, 10001):
        users_list.append({
            "user_id": i,
            "user_name": random.choice(names) + f"_{i}",
            "country": random.choice(countries),
            "subscription_type": random.choice(subs),
            "ingested_at": datetime.now().isoformat()
        })
        
    chunk_size = 2500
    for i in range(0, len(users_list), chunk_size):
        df_chunk = pd.DataFrame(users_list[i : i + chunk_size])
        csv_buffer = io.StringIO()
        df_chunk.to_csv(csv_buffer, index=False)
        csv_bytes = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
        
        destination_path = f"bronze/users/users_part_{i//chunk_size}.csv"
        s3_client.upload_fileobj(csv_bytes, BUCKET_NAME, destination_path)
        
    logger.info("Successfully generated and pushed partitioned user profiles to MinIO.")

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2026, 6, 26),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='lastfm_to_minio',
    default_args=default_args,
    schedule_interval='@once',  
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