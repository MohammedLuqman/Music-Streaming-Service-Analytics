from pyspark.sql import SparkSession

jars_to_load = [
    "/home/jovyan/jars/delta-spark_2.12-3.1.0.jar",
    "/home/jovyan/jars/delta-storage-3.1.0.jar",
    "/home/jovyan/jars/clickhouse-jdbc-0.6.0.jar",
    "/home/jovyan/jars/hadoop-aws-3.3.4.jar",
    "/home/jovyan/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/home/jovyan/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/home/jovyan/jars/kafka-clients-3.5.0.jar",
    "/home/jovyan/jars/commons-pool2-2.11.1.jar"
]

builder = SparkSession.builder \
    .appName("Production_Grade_Pipeline_vFinal") \
    .config("spark.jars", ",".join(jars_to_load)) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g")

spark = builder.getOrCreate()
print("Spark Session is UP")

import logging
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("BronzeLayer_Production")

logger.info("Initializing Optimized Bronze Layer")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("song_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True)
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "music_events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("ingestion_time", current_timestamp()) \
 .withColumn("source", lit("kafka_music_events"))

final_df = parsed_df.withColumn(
    "is_valid", 
    col("event_id").isNotNull() & col("user_id").isNotNull() & col("song_id").isNotNull()
)

valid_bronze = final_df.filter(col("is_valid") == True).drop("is_valid")
invalid_bronze = final_df.filter(col("is_valid") == False).withColumn("error_type", lit("MISSING_REQUIRED_FIELDS"))

valid_query = valid_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://landing-zone/checkpoints/bronze_v3_valid") \
    .partitionBy("event_type") \
    .start("s3a://landing-zone/bronze/events_valid/")

invalid_query = invalid_bronze.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://landing-zone/checkpoints/bronze_v3_invalid") \
    .start("s3a://landing-zone/bronze/events_invalid/")

logger.info("Bronze Layer: Pipeline running successfully.")

from pyspark.sql.functions import col, trim

df_songs_bronze = spark.read.option("multiLine", "true").json("s3a://landing-zone/bronze/songs/")
df_songs_dim = df_songs_bronze.select(
    trim(col("song_id")).alias("song_id"),
    col("song_name"),
    col("artist_name"),
    col("genre")
).distinct()

df_songs_dim.write.format("delta").mode("overwrite").save("s3a://landing-zone/silver/dim_songs/")
print("Songs Dimension populated successfully.")

df_users_bronze = spark.read.option("header", "true").csv("s3a://landing-zone/bronze/users/")
df_users_dim = df_users_bronze.select(
    col("user_id").cast("integer"),
    col("user_name"),
    col("country"),
    col("subscription_type"),
    lit(True).alias("is_current")
)

df_users_dim.write.format("delta").mode("overwrite").save("s3a://landing-zone/silver/dim_users/")
print("Users Dimension populated successfully.")

from pyspark.sql.functions import col, trim, current_timestamp

bronze_stream = spark.readStream.format("delta").load("s3a://landing-zone/bronze/events_valid/")

df_users = spark.read.format("delta").load("s3a://landing-zone/silver/dim_users/")
df_songs = spark.read.format("delta").load("s3a://landing-zone/silver/dim_songs/")

silver_enriched = bronze_stream.alias("events") \
    .join(df_users.alias("users"), "user_id", "left") \
    .join(df_songs.alias("songs"), trim(col("events.song_id")) == trim(col("songs.song_id")), "left") \
    .select(
        col("events.event_id"),
        col("events.user_id"),
        col("events.song_id"),
        col("users.user_name"),
        col("songs.song_name"),
        col("songs.artist_name"),
        col("songs.genre"),
        col("events.timestamp").alias("event_timestamp"),
        col("events.event_type"),
        col("users.subscription_type"),
        col("users.country"),
        current_timestamp().alias("processed_at") 
    )

silver_query = silver_enriched.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://landing-zone/checkpoints/silver_enriched_v3") \
    .start("s3a://landing-zone/silver/enriched_final/")

print("Silver Layer Streaming is now RUNNING")

from pyspark.sql.functions import col, to_timestamp, coalesce, lit
import pandas as pd
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GoldPipeline")

def write_to_clickhouse(df, batch_id):
    if df.isEmpty():
        return
    
    try:
        df_filtered = df.filter(col("event_id").isNotNull())        
        target_columns = [
            "event_id", "user_id", "song_id", "song_name", "artist_name", 
            "genre", "event_timestamp", "event_type", "subscription_type", 
            "country", "user_name", "processed_at"
        ]
        
        pdf = df_filtered.select(*target_columns).toPandas()
        
        for col_name in ['event_timestamp', 'processed_at']:
            if col_name in pdf.columns:
                pdf[col_name] = pd.to_datetime(pdf[col_name]).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        pdf = pdf.fillna("N/A")
                
        csv_data = pdf.to_csv(index=False, header=False, sep=',', quoting=1)
        
        url = "http://clickhouse:8123/?query=INSERT%20INTO%20default.realtime_music_events%20FORMAT%20CSV"
        
        response = requests.post(
            url, 
            data=csv_data.encode('utf-8'), 
            auth=('admin', 'admin_password'), 
            timeout=60
        )
        
        if response.status_code == 200:
            logger.info(f"[Batch {batch_id}] Successfully pushed {len(pdf)} rows to ClickHouse.")
        else:
            logger.error(f"[Batch {batch_id}] ClickHouse Error: {response.text}")
            
    except Exception as e:
        logger.error(f"[Batch {batch_id}] Error in write_to_clickhouse: {str(e)}")

silver_stream = spark.readStream.format("delta").load("s3a://landing-zone/silver/enriched_final/")

gold_df = silver_stream.select(
    col("event_id").cast("string"),
    col("user_id").cast("int"),
    col("song_id").cast("string"),
    coalesce(col("song_name"), lit("Unknown")).alias("song_name"),
    coalesce(col("artist_name"), lit("Unknown")).alias("artist_name"),
    coalesce(col("genre"), lit("Unknown")).alias("genre"),
    to_timestamp(col("event_timestamp")).alias("event_timestamp"),
    col("event_type").cast("string"),
    coalesce(col("subscription_type"), lit("Free")).alias("subscription_type"),
    coalesce(col("country"), lit("Unknown")).alias("country"),
    coalesce(col("user_name"), lit("Guest")).alias("user_name"),
    to_timestamp(col("processed_at")).alias("processed_at")
)

gold_query = gold_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "s3a://landing-zone/checkpoints/gold_final_v5") \
    .trigger(processingTime='10 seconds') \
    .start()

print("Gold Layer: Real-time pipeline is LIVE")
gold_query.awaitTermination()