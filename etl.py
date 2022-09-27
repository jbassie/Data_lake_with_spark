from datetime import datetime


from datetime import datetime
import os
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType, IntegerType

def create_Spark_Session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    #get filepath to song data file
    song_data = os.path.join(input_data, 'song_data', '*', "*", "*")

    #read song data file
    df = spark.read.json(song_data)

    #extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])

    #write songs table to parquet files partitioned by year
    songs_table.write.parquet(os.path.join*(output_data,'song'), partitionBy = ['year', 'artist_id'])

    #extract columns to create artits table
    columns = ['artist_name', 'artist_location','artist_latitude','artist_longitude']
    columns = [col + ' as '+ col.replace('artist_', "") for col in columns]
    artists_table = df.selectExpr('artist_id', *columns)

    #Write artists table to parquet files
    artists_table - df.selectExpr('artist_id', *columns)

    #write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists"))

def process_log_data(spark, input_data, output_data):
    #get filepath to log data file
    log_data  = os.path.join(input_data, 'log_data',"*","*")
    
    #read log data file
    df = spark.read.json(log_data)
    df = df.withColumn('user_id', df.userId.cast(IntegerType = ()))

    #filter by actions for song plays
    df  = df.where(df.page =="NextSong")

    #