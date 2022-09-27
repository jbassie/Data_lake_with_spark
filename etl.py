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
    return SparkSession

def process_song_data(spark, input_data, output_data):
    #get filepath to song data file
    song_data = 