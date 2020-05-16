import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,monotonically_increasing_id,dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Arguments: 
       - NONE
    Return:
       - spark - spark session variable
    
    Description 
       - create the spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Arguments: 
       - spark - spark session variable
       - input_data - path of the input files
       - output_data - path of the output files

    Return:
       - NONE
    
    Description 
       - Read the input files using the spark session as json files and create the tables songs and artists 
         then load the parquet files to s3 bucket sparkifyme with given output_data files
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'# for testing the local data upload to hdfs "song_data/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title','year','artist_id','duration').dropDuplicates()
    songs_table.createOrReplaceTempView('songs')
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs/songs.parquet'),'overwrite')

    # extract columns to create artists table
    artists_table =  df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.createOrReplaceTempView('artists')
    artists_table.write.parquet(os.path.join(output_data,'artists/artists.parquet'),'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Arguments: 
       - spark - spark session variable
       - input_data - path of the input files
       - output_data - path of the output files

    Return:
       - NONE
    
    Description 
       - Read the input files using the spark session as json files and create the tables songplays, users and
         time. Also convert the time in prope timestamp and date time. 
         then load the parquet files to s3 bucket sparkifyme with given output_data files
    """
    # get filepath to log data file
    log_data =input_data+'log-data/*/*/*.json' # for testing the local data upload to hdfs "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("log_data")
    # filter by actions for song plays
    df_filter = df.filter(df.page =='NextSong')

    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName','gender','level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users/users.parquet'),'overwrite')

    # create timestamp column from original timestamp column
    to_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df_filter = df_filter.withColumn('Timestamp',to_timestamp(df_filter.ts)) 
    
    # create datetime column from original timestamp column
    to_date = udf(lambda x : str(datetime.fromtimestamp(int(x)/1000)))
    df_filter = df_filter.withColumn('datetime',to_date(df_filter.ts)) 
    
    # extract columns to create time table
    time_table = df_filter.select('datetime').withColumn('start_time',df_filter.datetime)\
                                         .withColumn('hour',hour('datetime')) \
                                         .withColumn('day',dayofmonth('datetime')) \
                                         .withColumn('week',weekofyear('datetime')) \
                                         .withColumn('month',month('datetime')) \
                                         .withColumn('year',year('datetime')) \
                                         .withColumn('weekday',dayofweek('datetime')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'times/times.parquet'),'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    filter_df = df_filter.alias('log')
    song_df = song_df.alias('song')
    inner_join = filter_df.join(song_df, col('log.artist') == col('song.artist_name'), 'inner')
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = inner_join.select(
                  col('log.datetime').alias('start_time'),
                  col('log.userId').alias('user_id'),
                  col('log.level').alias('level'),
                  col('song.song_id').alias('song_id'),
                  col('song.artist_id').alias('artist_id'),
                  col('log.sessionId').alias('session_id'),
                  col('log.location').alias('location'),
                  col('log.userAgent').alias('user_agent'),
                  year('log.datetime').alias('year'),
                  month('log.datetime').alias('month')).withColumn('songplay_id',monotonically_increasing_id()).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'songplays/songplays.parquet'),'overwrite')
def main():
    spark = create_spark_session()
    input_data =  # for the full dataset in s3 "s3a://udacity-dend/"
    output_data = "s3a://sparkifyme/local_output/" # s3 bucket created named sparkifyme
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
