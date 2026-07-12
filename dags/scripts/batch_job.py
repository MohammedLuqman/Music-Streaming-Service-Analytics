from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lit
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Production_Grade_Pipeline") \
    .config("spark.jars", "/home/jovyan/jars/*") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()


def upsert_to_delta(df, target_path, merge_condition):
    if DeltaTable.isDeltaTable(spark, target_path):
        delta = DeltaTable.forPath(spark, target_path)
        delta.alias("target").merge(
            df.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        df.write.format("delta").save(target_path)

songs = spark.read.option("multiLine", "true").json("s3a://landing-zone/bronze/songs/")
songs_new = songs.select(
    trim(col("song_id")).alias("song_id"),
    col("song_name"),
    col("artist_name"),
    col("genre")
).dropDuplicates(["song_id"])

upsert_to_delta(
    songs_new, 
    "s3a://landing-zone/silver/dim_songs/", 
    "target.song_id = source.song_id"
)

users = spark.read.option("header", "true").csv("s3a://landing-zone/bronze/users/")
users_new = users.select(
    col("user_id").cast("integer"),
    col("user_name"),
    col("country"),
    col("subscription_type"),
    lit(True).alias("is_current")
).dropDuplicates(["user_id"])

upsert_to_delta(
    users_new, 
    "s3a://landing-zone/silver/dim_users/", 
    "target.user_id = source.user_id"
)

print("Silver Dimensions Finished")
spark.stop()