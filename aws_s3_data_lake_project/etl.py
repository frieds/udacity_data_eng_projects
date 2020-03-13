import configparser
from os import environ
from boto3 import resource
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, monotonically_increasing_id, isnan, year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, to_timestamp)

config = configparser.ConfigParser()
config.read('dl.cfg')

environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def etl_song_data(spark, s3_input_bucket_url):
    """
    Read in S3 log files from udacity-dend bucket by song_data prefix, do transformations in PySpark to create dataframes
    to represent star schema tables of dimensions for songs and artists. If new data is found in PySpark dataframe 
    for one of these tables that doesn't exist in s3 output bucket, we load it to S3 in the output bucket. 
    
    param spark: spark session object
    param s3_input_bucket_url: s3 Udacity provided input bucket string for Sparkify logs at bucket udacity-dend
    return df_song_logs: PySpark dataframe of all records under prefix of song_data in input bucket
    """
    print("read in song longs with any filename under path with exact # of slashes and ends with .json")
    s3_input_song_longs_prefix = "song_data/*/*/*/*.json"
    s3_song_data_url = s3_input_bucket_url + s3_input_song_longs_prefix
    df_song_logs = spark.read.json(s3_song_data_url)
        
    # SONGS dimension table
    """In some logs, a missing value could be a blank string or be a Null/None. To catch both, we'll filter out missing values 
    by both ways. Exract relevant columns. Filter for non-missing song_id values because that's our primary key here and primary 
    keys must contain a value. Filter for non-missing artist_id values because that'll be a foreign key needed to join to the 
    artists table. Filter for unique song ids because it's a dimension table and we only want unique values here."""
    print("perform transformations on df_song_logs to convert it to a relevant dim table - a PySpark dataframe called df_songs")
    df_songs = df_song_logs.select("song_id", "title", "artist_id", "year", "duration")\
                 .filter((df_song_logs['song_id'] != "") & (df_song_logs['song_id'].isNotNull()) &\
                 (df_song_logs['artist_id'] != "") & (df_song_logs['artist_id'].isNotNull()))\
                 .dropDuplicates(['song_id'])

    """Upon uploading transformed data to s3, we want to partition it by artist and year. We're OK with missing values in these 
    fields. But, we don't want these values to be blank when uploading to S3 because that may be confusing. Therefore, let's 
    impute values for blank entries of artist and year. I'll fill those blank values with 'XXXX'"""
    df_songs = df_songs.fillna("XXXX", subset=['artist', 'year'])
    
    print("let's print details on df_songs ...")
    inspect_dataframe(df_songs)
    
    print("all conditionally_load_data_to_s3 function with arguments specific to songs")
    conditionally_load_data_to_s3(data_category="songs",
                                  spark=spark,
                                  dataframe=df_songs,
                                  partition_s3_upload_columns_list=["year", "artist_id"],
                                  primary_key="song_id"
                                 )
    # ARTISTS dimension table
    print("perform transformations on df_song_logs to convert it to a relevant dim table - a PySpark dataframe called df_songs")
    df_artists = df_song_logs.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")\
                   .filter((df_songs['artist_id'] != "") & (df_songs['artist_id'].isNotNull()))\
                   .dropDuplicates(["artist_id"])

    print("print details on df_artists ...")
    inspect_dataframe(df_artists)

    print("call conditionally_load_data_to_s3 function with arguments specific to artists")
    conditionally_load_data_to_s3(data_category="artists",
                                  spark=spark,
                                  dataframe=df_artists,
                                  partition_s3_upload_columns_list=[],
                                  primary_key="artist_id"
                                 )
    return df_song_logs

def elt_log_data(spark, s3_input_bucket_url, df_song_logs):
    """
    Read in S3 log files from udacity-dend bucket by log-data prefix, do transformations in PySpark to create dataframes
    to represent star schema tables of dimensions for time and users and a fact of songplays. If new data is found in 
    PySpark dataframe for one of these tables that doesn't exist in s3 output bucket, we load it to S3 in the output bucket. 

    param spark: spark session object
    param s3_input_bucket_url: s3 Udacity provided input bucket string for Sparkify logs at bucket udacity-dend
    param df_song_logs: PySpark dataframe of all records in udacity-dend input bucket with prefix song_data
    return None: 
    """
    print("get all data in the log-data prefix and assign to PySpark df_log_data dataframe...")
    s3_log_data_raw_prefix = "log-data/*/*/*.json"
    s3_full_path_log_data = s3_input_bucket_url + s3_log_data_raw_prefix
    df_log_data = spark.read.json(s3_full_path_log_data)

    print("print details on df_log_data...")
    inspect_dataframe(df_log_data)

    # TIME dimension table
    """
    ts is of type long. We want to convert it to a datetime type. Divide unix timestamp by 1000. Use from_unixtime function to convert 
    unix timestamp into a string with specific datetime formatting. Use to_timestamp to convert column to timestamp type. Filter 
    where page is equal to NextSong. Use built-in PySpark functions to extract attributes of the timestamp. See Java simple date 
    format options to get weekday int value: https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
    """
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
    df_time = df_log_data.filter(df_log_data['page'] == 'NextSong')\
                         .drop_duplicates(subset=['ts'])\
                         .select("ts")\
                         .withColumn('start_time', to_timestamp(from_unixtime(df_log_data.ts/1000, timestamp_format), timestamp_format))\
                         .withColumn('hour', hour('start_time'))\
                         .withColumn('day', dayofmonth('start_time'))\
                         .withColumn('week', weekofyear('start_time'))\
                         .withColumn('month', month('start_time'))\
                         .withColumn('year', year('start_time'))\
                         .withColumn('weekday', date_format('start_time', 'u'))\
                         .drop("ts")

    print("print details of df_time...")
    inspect_dataframe(df_time)

    print("call conditionally_load_data_to_s3 function with arguments specific to time data")
    conditionally_load_data_to_s3(data_category="time",
                                spark=spark,
                                dataframe=df_time,
                                partition_s3_upload_columns_list=["year", "month"],
                                primary_key="start_time"
                                )

    # USERS dimension table
    print("perform transformations on df_log_data to convert it to a relevant dim table - a PySpark dataframe called df_users")
    # get users by max ts record because that'll be their most up to date level - level being if a paid or free user of Sparkify
    df_users = df_log_data.groupby("userId", "firstName", "lastName", "gender", "level")\
                          .max("ts")\
                          .drop("max(ts)")

    print("print details of df_users ...")
    inspect_dataframe(df_users)

    print("call conditionally_load_data_to_s3 function with arguments specific to users")
    conditionally_load_data_to_s3(data_category="users",
                                  spark=spark,
                                  dataframe=df_users,
                                  partition_s3_upload_columns_list=[],
                                  primary_key="userId"
                                  )

    # SONGPLAYS fact table
    """We need to join df_log_data and df_song_logs and gather relevant columns. Convert ts into a datetime type and column called start_time.
    Create a monotonically increasing id value that's our primary key. Filter df_log_data for values in which ts and userId are not empty 
    strings because they are foreign keys which we use to join to other tables."""
    df_songplays = df_log_data.filter((~isnan(df_log_data["ts"])) & (df_log_data['userId'] != "") & (df_log_data['page'] == 'NextSong'))\
                              .join(df_song_logs, (df_log_data.artist == df_song_logs.artist_name) &\
                              (df_log_data.length == df_song_logs.duration) &\
                              (df_log_data.song == df_song_logs.title), how='left')\
                              .select("ts", "userId", "level", "artist", "song_id", "artist_id", "sessionId", "location", "userAgent")\
                              .withColumn('start_time', to_timestamp(from_unixtime(df_log_data.ts/1000, timestamp_format), timestamp_format))\
                              .withColumn('month', month('start_time'))\
                              .withColumn('year', year('start_time'))\
                              .drop("ts")\
                              .withColumn('id', monotonically_increasing_id())
    
    print("print details of df_songplays ...")
    inspect_dataframe(df_songplays)

    print("call conditionally_load_data_to_s3 function with arguments specific to songplays")
    conditionally_load_data_to_s3(data_category="songplays",
                                  spark=spark,
                                  dataframe=df_songplays,
                                  partition_s3_upload_columns_list=["year", "month"],
                                  primary_key="id"
                                 )
    return None

def inspect_dataframe(df):
    """
    Print out details regarding a (hopefully) PySpark dataframe object
    param df: PySpark dataframe object
    return None
    """
    print("Verify type of object as a Spark dataframe...")
    print(type(df))
    
    print("View schema - the names of columns and data types...")
    print(df.printSchema())
    
    print("View first two records...")
    print(df.take(2))
    return None


def upload_dataframe_to_s3(partition_s3_upload_columns_list, dataframe, s3_full_path_to_transformed_category_prefix):
    """
    upload pyspark dataframe to s3 based on any partition columns and s3 transformed data category prefix

    param dataframe: PySpark dataframe of data collected that fits transformations for proper dim or fact table by data_category
    param partition_s3_upload_columns_list: python list of columns in dataframe that will be s3 partitions after transformed/data_category in output bucket; could be empty list if no partitions needed
    param s3_full_path_to_transformed_category_prefix: s3 location for output bucket and transformed/{data_category_name}
    return None
    """
    
    if len(partition_s3_upload_columns_list) >= 0:
        dataframe.write.parquet(s3_full_path_to_transformed_category_prefix, partitionBy=partition_s3_upload_columns_list)
    else:
        dataframe.write.parquet(s3_full_path_to_transformed_category_prefix)
    return None


def conditionally_load_data_to_s3(data_category, spark, dataframe, partition_s3_upload_columns_list, primary_key):
    """
    Given a PySpark dataframe of records retrieved from our input bucket, compare this data by the category it represents 
    in the output bucket; identify if any new records exist in dataframe that aren't present in output bucket by prefix; if so,
    upload data to s3 with specfications for s3 partition columns and s3 prefix
    
    param data_category: string to represent prefix in S3 to appear after transformed/ prefix in output bucket
    param spark: spark session object
    param dataframe: PySpark dataframe of data collected that fits transformations for proper dim or fact table by data_category
    param partition_s3_upload_columns_list: python list of columns in dataframe that will be s3 partitions after transformed/data_category in output bucket; could be empty list if no partitions needed
    param primary_key: primary key of dataframe
    return None    
    """
    # assign variables for s3 related output bucket details
    s3_output_bucket_name = "sparkify-data-lake3"
    s3_output_bucket_url = f"s3a://{s3_output_bucket_name}/"
    s3_output_prefix = f"transformed/{data_category}/"
    s3_full_path_to_transformed_category_prefix = s3_output_bucket_url + s3_output_prefix

    # Establish boto3 s3 object to programatically retrieve data from s3 output bucket
    s3 = resource('s3', 
                  region_name="us-west-2", 
                  aws_access_key_id=environ['AWS_ACCESS_KEY_ID'], 
                  aws_secret_access_key=environ['AWS_SECRET_ACCESS_KEY']
                 )

    bucket_boto3_object = s3.Bucket(s3_output_bucket_name)

    print("get all s3 objects in the prefix of our bucket for transformed data...")
    s3_objects_collection_in_category = bucket_boto3_object.objects.filter(Prefix=s3_output_prefix)

    # if nothing is in the bucket, all data received from our raw bucket is unique; write all data to S3
    if len(list(s3_objects_collection_in_category)) == 0:
        print("no records in s3 exist by prefix; write all data from input bucket now in PySpark dataframe to s3 output bucket")
        upload_dataframe_to_s3(partition_s3_upload_columns_list, dataframe, s3_full_path_to_transformed_category_prefix)
    else:
        print("find if any data from s3 input bucket doesn't exist in s3 output bucket...")
        print("if new data exists, upload to s3 output bucket; if not, do nothing")
        primary_keys_list = []
        print("iterate over all object resources in s3 bucket w/prefix")
        for s3_object in s3_objects_collection_in_category:
            print(f"key name: {s3_object.key}")
            # only want to analyze non-"_SUCCESS" objects so s3 object must have string partition names in it
            if "SUCCESS" not in s3_object.key:
                # transformed_table is all data in output bucket by s3 prefix for data category
                transformed_table = spark.read.parquet(f"{s3_output_bucket_url}{s3_object.key}")
                # get python list of list for primary key values in transformed_table
                primary_key_values = transformed_table.select(primary_key).toPandas()[primary_key].tolist()
                # primary_key_values is a list of lists and we need to append each item to primary_keys_list
                for primary_key_value in primary_key_values:
                    primary_keys_list.append(primary_key_value)

        # only keep records of dataframe that don't exist in transformed/{category} prefix records by primary key value
        dataframe = dataframe.filter(~col(primary_key).isin(primary_keys_list))
        print(f"we've filtered by the unique primary key: {primary_key}")
        
        print("print details of dataframe...")
        inspect_dataframe(dataframe)

        # if dataframe has records, write the data to S3 output bucket using specified s3 partitions and prefix
        if dataframe.count() != 0:
            print("write new records to s3 output bucket...")
            upload_dataframe_to_s3(partition_s3_upload_columns_list, dataframe, s3_full_path_to_transformed_category_prefix)
        else:
            print("nothing new to upload to s3 output bucket")


def main():
    spark = create_spark_session()
    """We're running open-source Spark (i.e. no proprietary Amazon libraries) built on Hadoop 2.7 or newer so our s3 path 
    should start with `s3a://` https://stackoverflow.com/questions/27477730/how-to-read-multiple-gzipped-files-from-s3-into-a-single-rdd"""
    s3_input_bucket_url = "s3a://udacity-dend/"

    df_song_logs = etl_song_data(spark, s3_input_bucket_url)    
    elt_log_data(spark, s3_input_bucket_url, df_song_logs)


if __name__ == "__main__":
    main()