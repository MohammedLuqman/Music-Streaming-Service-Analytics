import json
import random
import io
import boto3
from botocore.client import Config
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
import numpy as np

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "minio_password"
BUCKET_NAME = "landing-zone"
    
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092'] 
KAFKA_TOPIC = 'music_events'
TOTAL_EVENTS_TO_SEND = 1000   

def fetch_ids_from_minio():
    s3_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, 
                             aws_access_key_id=MINIO_ACCESS_KEY, 
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             config=Config(signature_version='s3v4'))
    
    all_user_ids = []
    try:
        user_files = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="bronze/users/")
        for obj in user_files.get('Contents', []):
            if obj['Key'].endswith('_SUCCESS') or obj['Size'] == 0: continue
            u_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=obj['Key'])
            df_chunk = pd.read_csv(io.BytesIO(u_obj['Body'].read())) if not obj['Key'].endswith('.parquet') else pd.read_parquet(io.BytesIO(u_obj['Body'].read()))
            if 'user_id' in df_chunk.columns: all_user_ids.extend(df_chunk['user_id'].tolist())
    except: pass

    all_song_ids = []
    try:
        song_files = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="bronze/songs/")
        for obj in song_files.get('Contents', []):
            if obj['Key'].endswith('_SUCCESS') or obj['Size'] == 0: continue
            s_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=obj['Key'])
            s_data = json.loads(s_obj['Body'].read().decode('utf-8'))
            if isinstance(s_data, list): all_song_ids.extend([s['song_id'] for s in s_data if 'song_id' in s])
            elif isinstance(s_data, dict) and 'song_id' in s_data: all_song_ids.append(s_data['song_id'])
    except: pass

    return list(set(all_user_ids or [1, 2, 3, 4, 5])), list(set(all_song_ids or ["song_1", "song_2", "song_3"]))

def get_power_law_weights(n, power=2.5):
    weights = [1 / (i ** power) for i in range(1, n + 1)]
    return [w / sum(weights) for w in weights]

def start_bounded_streaming():
    user_ids, song_ids = fetch_ids_from_minio()
    song_weights = get_power_law_weights(len(song_ids), power=2.5)
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    base_avg_duration = 180 
    
    for i in range(TOTAL_EVENTS_TO_SEND):
        trend_factor = 60 * np.sin(i / 30) 
        
        event_type = random.choices(['PLAY', 'SKIP', 'LIKE'], weights=[0.7, 0.2, 0.1])[0]
        
        if event_type == 'PLAY':
            duration = max(30, int(base_avg_duration + trend_factor + random.uniform(-40, 40)))
        else:
            duration = random.randint(5, 25)
            
        event = {
            "event_id": str(random.randint(1000000, 9999999)),
            "user_id": int(random.choice(user_ids)),
            "song_id": str(random.choices(song_ids, weights=song_weights, k=1)[0]),
            "duration_seconds": duration,
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "event_type": event_type
        }
        producer.send(KAFKA_TOPIC, value=event)
            
    producer.flush()
    producer.close()
    print("Streaming completed")

if __name__ == "__main__":
    start_bounded_streaming()