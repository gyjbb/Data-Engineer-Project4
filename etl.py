import configparser
from datetime import datetime
import os
from os.path import join, dirname, abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType



def create_spark_session():
    """
    create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def create_song_dataframe(input_data, spark):
    """
    This function uses spark to read the song data and create a song dataframe for further data analysis.
    """
    # read the song_data
    song_data = input_data + "/song_data/*/*/*/"
    songdata_schema = StructType([
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True)
    ])
    song_df = spark.read.json(song_data, schema=songdata_schema)
    return song_df

def create_log_dataframe(input_data, spark):
    """
    This function uses spark to read the log data and create a log dataframe for further data analysis.
    """
     # read the log_data
    log_data = input_data + "/log_data/*/*/*/"
    logdata_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    log_df = spark.read.json(log_data, schema=logdata_schema)
    # create a new timestamp column for the log dataframe
    create_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    log_df = log_df.withColumn('timeStamp', create_timestamp(log_df.ts))
    # creaate a new start time column for the log dataframe
    log_df = log_df.withColumn('startTime', str(log_df.timeStamp))
    return log_df


def process_song_data(spark, input_data, output_data):
    """
    This function reads the songs dataset and creates the songs_table and artists table for data ETL.
    """
    # get filepath to song data file
    song_data = create_song_dataframe(input_data, spark)
    song_data.createOrReplaceTempView("staging_songs")

    # extract columns to create songs table
    songs_table = spark.sql("""SELECT DISTINCT song_id, title \
                            artist_id, year, duration 
                            FROM staging_songs
                            where year != 0
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.createOrReplaceTempView('songs')
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""SELECT DISTINCT artist_id, artist_name, artist_location, \
                            artist_latitude, artist_longitude 
                            FROM staging_songs
    """)
    
    # write artists table to parquet files
    artists_table.createOrReplaceTempView('artists')
    artists_table.write.parquet(
        join(output_data, 'artists/artists.parquet'), 'overwrite'
    )


def process_log_data(spark, input_data, output_data):
    """
    This function firstly reads the logs dataset, and creates the users table, and time table.
    Then it loads the songs dataset, join two datasets together and create the songplay table.
    """
    # get filepath to log data file
    log_data = create_log_dataframe(input_data, spark)
    # filter by actions for song plays
    log_data = log_data.filter(log_data.page == "NextSong")
    log_data.createOrReplaceTempView("staging_logs")

    # extract columns for users table    
    users_table = spark.sql("""SELECT DISTINCT userId, firstName, \
                            lastName, gender, level
                            FROM staging_logs
    """)
    
    # write users table to parquet files
    users_table.createOrReplaceTempView('users')
    users_table.write.parquet(
        join(output_data, 'users/users.parquet'), 'overwrite')
    
    # extract columns to create time table
    time_table = spark.sql("""SELECT startTime, 
                            hour(startTime) AS hour, 
                            day(startTime) AS day, 
                            weekofyear(startTime) AS week, 
                            month(startTime) AS month, 
                            year (startTime) AS year, 
                            weekday(startTime) AS weekday 
                            FROM staging_logs
    """)
    
    # write time table to parquet files partitioned by year and month
    #time_table.createOrReplaceTempView('time')
    time_table.createOrReplaceTempView('time')
    time_table.write.partitionBy(
        'year', 'month') \
        .parquet(join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = create_song_dataframe(input_data, spark)
    combined_df = spark.sql("""SELECT l.userId AS userId, l.level AS level, \
                s.song_id AS song_id, s.artist_id AS artist_id, \
                l.sessionId AS sessionId, l.location AS location, l.userAgent AS userAgent, \
                l.startTime AS startTime
                FROM staging_logs l
                INNER JOIN song_df s
                ON staging_logs.artist = song_df.artist_name
    """)

    # extract columns from joined song and log datasets to create songplays table 
    combined_df.createOrReplaceTempView("combined_table")
    songplays_table = spark.sql("""SELECT startTime, userId, level, song_id, artist_id, \
                sessionId, location, userAgent, \
                year(startTime) AS year, month(startTime) AS month
                FROM combined_table
    """).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.createOrReplaceTempView('songplays')
    songplays_table.write.\
        partitionBy('year', 'month').\
        parquet(join(output_data, 'songplays/songplays.parquet'), 'overwrite')


def main():
    """
    The main function define the directories for input and output data,
    and call the ETL functions to process datasets.
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    root_dir = dirname(abspath(__file__))
    output_data = "root_dir + '/data/'"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
