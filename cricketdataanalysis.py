from pyspark.sql import SparkSession
from pyspark.sql.types import *
import uuid
from pyspark.sql.functions import lit, explode, col, expr

import logging


s3_bucket_name = "s3a://cricketdataanalysis/"
metadata_file = "raw_match_files/metadata.txt"

# Function to create SparkSession and return the SparkSession object created
def create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName("CricketDataAnalysis").master("local[*]").getOrCreate()

# Function to read the metadata.txt file to get the list of files that needs to be processed
def get_files_detail(spark: SparkSession):
    files_df = spark.read.text(f"{s3_bucket_name}{metadata_file}")
    files_str = files_df.collect()[0][0]
    files_list = files_str.split(',')
    logging.info(f"Files List: {files_list}")
    return files_list

# Function ot extract match dimension data from the file and load it to as parquet
def process_match_data(spark: SparkSession, file: str, match_id: str, raw_match_df):

    # I wanted to extract a key value from the json. Usually when key: value pairs are present in json we use the key like json[key] to the value, But what if I want the key value itself.
    # For that I am getting the schema info and then getting the required details
    players_struct_schema =  raw_match_df.schema['info'].dataType['players'].dataType
    country_names = [field.name for field in players_struct_schema.fields]
    teamA = country_names[0]
    teamB = country_names[1]

    
    # As this was the contexual attributes, there was no need to perform perform,these were directly available at root level of json
    match_df = raw_match_df.withColumn("MATCH_ID", lit(match_id))\
        .withColumn("CITY", raw_match_df['info'].city)\
        .withColumn("DATES",raw_match_df['info'].dates)\
        .withColumn("EVENT_NAME",raw_match_df['info']['event']['name'])\
        .withColumn("PLAYERS_GENDER",raw_match_df['info'].gender)\
        .withColumn("MATCH_TYPE",raw_match_df['info'].match_type)\
        .withColumn("MATCH_TYPE_NUMBER",raw_match_df['info'].match_type_number)\
        .withColumn("MATCH_REFEREES",raw_match_df['info']['officials']['match_referees'])\
        .withColumn("TV_UMPIRES",raw_match_df['info']['officials']['tv_umpires'])\
        .withColumn("UMPIRES",raw_match_df['info']['officials']['umpires'])\
        .withColumn("RESULT",raw_match_df['info']['outcome'])\
        .withColumn("PLAYER_OF_MATCH",raw_match_df['info']['player_of_match'])\
        .withColumn("TEAM_A",lit(teamA))\
        .withColumn("TEAM_B",lit(teamB))\
        .withColumn("TEAM_A_PLAYERS", raw_match_df['info']['players'][f'{teamA}'])\
        .withColumn("TEAM_B_PLAYERS", raw_match_df['info']['players'][f'{teamB}'])\
        .withColumn("SEASON",raw_match_df['info']['season'])\
        .withColumn("TEAM_TYPE",raw_match_df['info']['team_type'])\
        .withColumn("TOSS_WINNER",raw_match_df['info']['toss']['decision'])\
        .withColumn("TOSS_DECISION",raw_match_df['info']['toss']['winner'])\
        .withColumn("VENUE",raw_match_df['info']['venue'])\
        .withColumn("FILE_NAME", lit(file))\
        .drop("info","innings","meta")

    # Saving the df as parquet
    match_df.write.format('parquet').save(f"{s3_bucket_name}processed_match_files/match_{match_id}")

# This function is to get the innings fact data from the file    
def process_innings_data(spark: SparkSession, file: str, match_id: str, raw_match_df):
    # Here I need to perform explode as it was similar repeating data
    flattened_df = raw_match_df.select(lit(match_id).alias("match_id"),col("innings"))\
            .select("match_id",explode(raw_match_df['innings']).alias("innings"))\
            .select("match_id",col("innings")['team'].alias("team"),explode(col("innings")['overs']).alias("overs"))\
            .select("match_id","team",col("overs")['over'].alias("over"),explode(col("overs")['deliveries']).alias("deliveries"))\
            .select("match_id","team","over",col("deliveries")['batter'].alias("batter"),col("deliveries")['bowler'].alias("bowler"),col("deliveries")['non_striker'].alias("non_striker"),col("deliveries")['runs']['batter'].alias("batter_runs"),col("deliveries")['runs']['extras'].alias("extras_runs"),col("deliveries")['wickets'].alias("wickets"))
    # I am using expr to perform conditional logic using sql like text
    innings_df = flattened_df.withColumn("is_wicket", expr("CASE WHEN wickets IS NULL THEN 'False' ELSE 'True' END"))\
            .withColumn("player_out",col("wickets")[0]['player_out'])\
            .withColumn("wicket_type",col("wickets")[0]['kind'])\
            .withColumn("FILE_NAME", lit(file))

    # Saving the df as parquet
    innings_df.write.format('parquet').save(f"{s3_bucket_name}processed_match_files/innings_{match_id}")

def main():

    # Using the pythons logging module to log info in a file for debugging purposes
    logging.basicConfig(
    filename='my_application.log', # Specify the log file name
    filemode='a',                  # 'a' for append (default), 'w' for overwrite
    level=logging.INFO,            # Set the minimum level to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    spark = create_spark_session("CricketDataAnalysis")
    logging.info("spark session created")

    files_list = get_files_detail(spark)

    # Process each file present in the bucket
    for file in files_list:
        logging.info(f"Processing file: {file}")
        # generate unique id to create relationship between match dimension and innings fact
        match_id = str(uuid.uuid4())
        raw_match_df = spark.read.option("multiLine","true").json(f"{s3_bucket_name}{file}")

        # The raw_df is used at two places, one to get match data another place to get innings data. Hence it will be read from source twice.
        # To avoid reading from source twice, cache it the first time, so second time it is read from cache.
        raw_match_df.cache().count()
        
        process_match_data(spark, file, match_id, raw_match_df)
        logging.info(f" match data processed for file: {file}")

        process_innings_data(spark, file, match_id, raw_match_df)
        logging.info(f" Innings data processed for file: {file}")


if __name__ == "__main__":
    main()