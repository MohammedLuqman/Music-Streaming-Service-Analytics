CREATE TABLE IF NOT EXISTS default.realtime_music_events (
    event_id String,
    user_id Int32,
    user_name String,
    song_id String,
    song_name String,
    artist_name String,
    genre String,
    country String,
    event_type String,    
    event_timestamp DateTime,  
    processed_at DateTime     
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (event_type, event_timestamp, user_id);