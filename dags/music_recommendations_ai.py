from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import json

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_and_push_recommendations():
    ch_url = "http://clickhouse:8123/"
    ch_auth = ('admin', 'admin_password')
    
    users_query = "SELECT DISTINCT user_id, user_name, country FROM default.realtime_music_events WHERE user_id > 0 AND user_name != ''"
    top_songs_query = """
        SELECT country, song_name, artist_name, count() as plays 
        FROM default.realtime_music_events 
        WHERE event_type = 'PLAY' AND song_name != ''
        GROUP BY country, song_name, artist_name
        ORDER BY country, plays DESC
    """
    
    try:
        res_users = requests.get(f"{ch_url}?query={users_query} FORMAT JSONEachRow", auth=ch_auth)
        users = [json.loads(line) for line in res_users.text.strip().split('\n') if line]
        
        res_songs = requests.get(f"{ch_url}?query={top_songs_query} FORMAT JSONEachRow", auth=ch_auth)
        songs = [json.loads(line) for line in res_songs.text.strip().split('\n') if line]
        
        if not users or not songs:
            print("No data found")
            return
            
        df_users = pd.DataFrame(users)
        df_songs = pd.DataFrame(songs)
        
        recommendations_list = []
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for _, user in df_users.iterrows():
            u_id = int(user['user_id'])
            u_name = str(user['user_name'])
            u_country = user['country']
            
            country_songs = df_songs[df_songs['country'] == u_country]
            
            if country_songs.empty:
                top_3 = df_songs.head(3)
            else:
                top_3 = country_songs.head(3)
                
            recommended_array = [f"{row['song_name']} - {row['artist_name']}" for _, row in top_3.iterrows()]
            
            recommendations_list.append({
                "user_id": u_id,
                "user_name": u_name, 
                "recommended_songs": recommended_array,
                "updated_at": current_time
            })
            
        json_lines = "\n".join([json.dumps(row) for row in recommendations_list])
        
        insert_url = f"{ch_url}?query=INSERT%20INTO%20default.user_recommendations%20FORMAT%20JSONEachRow"
        
        response = requests.post(insert_url, data=json_lines.encode('utf-8'), auth=ch_auth)
        
        if response.status_code == 200:
            print(f"AI Recommendations updated successfully")
        else:
            print(f"Insert Error: {response.text}")
            
    except Exception as e:
        print(f"Failed to run {e}")

with DAG(
    'music_realtime_recommendations_ai',
    default_args=default_args,
    description=' AI Recommendation Engine',
    schedule_interval='0 * * * *', 
    catchup=False,
    max_active_runs=1
) as dag:

    ai_recommendation_task = PythonOperator(
        task_id='generate_user_recommendations',
        python_callable=generate_and_push_recommendations,
    )

    ai_recommendation_task