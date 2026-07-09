import pandas as pd
import requests
import json
from datetime import datetime

CH_URL = "http://clickhouse:8123/"
CH_AUTH = ('admin', 'admin_password')

def generate_and_push_recommendations():
    users_query = "SELECT DISTINCT user_id, user_name, country FROM default.realtime_music_events WHERE user_id > 0 AND user_name != ''"
    top_songs_query = """
        SELECT country, song_name, artist_name, count() as plays 
        FROM default.realtime_music_events 
        WHERE event_type = 'PLAY' AND song_name != ''
        GROUP BY country, song_name, artist_name
        ORDER BY country, plays DESC
    """
    
    try:
        res_users = requests.get(f"{CH_URL}?query={users_query} FORMAT JSONEachRow", auth=CH_AUTH)
        users = [json.loads(line) for line in res_users.text.strip().split('\n') if line]
        
        res_songs = requests.get(f"{CH_URL}?query={top_songs_query} FORMAT JSONEachRow", auth=CH_AUTH)
        songs = [json.loads(line) for line in res_songs.text.strip().split('\n') if line]
        
        if not users or not songs:
            print("No data found to process")
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
            top_3 = country_songs.head(3) if not country_songs.empty else df_songs.head(3)
                
            recommended_array = [f"{row['song_name']} - {row['artist_name']}" for _, row in top_3.iterrows()]
            
            recommendations_list.append({
                "user_id": u_id,
                "user_name": u_name, 
                "recommended_songs": recommended_array,
                "updated_at": current_time
            })
            
        json_lines = "\n".join([json.dumps(row) for row in recommendations_list])
        insert_url = f"{CH_URL}?query=INSERT%20INTO%20default.user_recommendations%20FORMAT%20JSONEachRow"
        
        response = requests.post(insert_url, data=json_lines.encode('utf-8'), auth=CH_AUTH)
        
        if response.status_code == 200:
            print("AI Recommendations updated successfully")
        else:
            print(f"Insert Error: {response.text}")
            
    except Exception as e:
        print(f"Failed to run: {e}")
        raise e