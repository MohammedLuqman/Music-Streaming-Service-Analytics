CREATE TABLE default.user_recommendations (
    user_id Int32,
    user_name String,                 
    recommended_songs Array(String),    
    updated_at DateTime                  
) ENGINE = ReplacingMergeTree()
PRIMARY KEY user_id;