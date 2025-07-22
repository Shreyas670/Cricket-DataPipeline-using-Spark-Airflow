from airflow.sdk import dag, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook




import csv

s3_bucket_name = "cricketdataanalysis"
prefix = "raw_match_files/"

# Credentials required for Spark to connect with AWS. These credentials will be sent as env config for spark.
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""


#Create DAG using dag decorator
@dag
def cricket_data_analysis():
    # A sensor to wait for the files to be loaded to the raw s3 bucket folder.
    @task.sensor(poke_interval=30, timeout=300)
    def is_file_available() -> PokeReturnValue: 
        
       s3_hook=S3Hook(aws_conn_id='aws_s3_conn') 

       # Get the list of files present in the bucket
       keys = s3_hook.list_keys(bucket_name=s3_bucket_name, prefix=prefix)
       keys.remove(prefix)  # Remove the entry with empty folder name
       keys.remove("raw_match_files/metadata.txt") # Remove the metadata file
       print(f"files:{keys}")

       files_str = ",".join(keys)
       # Rewrite Metadata file with the file names present in the folder
       s3_hook.load_string(string_data=files_str,
       key="raw_match_files/metadata.txt",
       bucket_name=s3_bucket_name,
       replace=True)

       print("Metadata written back to S3")
     
       # If one or more files are present other than metadata file, send pokereturnvalue as True and mark the sensor as success. Also returns xcom value of the files list to be used in downstream tasks
       if keys:
            condition = True
            files_list = keys
       else:
            condition = False
            files_list = None
       return PokeReturnValue(is_done=condition, xcom_value=files_list)
    
    # Spark submit operator to submit spark job to a spark cluster connected using conn_id. Requires additional config and packages as spark is running on client mode where master process runs in airflow worker.
    submit_spark_job = SparkSubmitOperator(
        task_id='submit_my_spark_job',
        application='./dags/cricketdataanalysis.py',  
        conn_id='spark_default',
        conf={'spark.hadoop.fs.s3a.access.key': AWS_ACCESS_KEY_ID,
            'spark.hadoop.fs.s3a.secret.key': AWS_SECRET_ACCESS_KEY},
        packages='org.apache.hadoop:hadoop-aws:3.4.1'
    )

    # Once spark processes the files and loads it to a processed folder, trigger snowflake procedures to load and restructure the data
    snowflake_load = SQLExecuteQueryOperator(
        task_id="snowflake_load",
        conn_id="snowflake_default",
        autocommit = True,
        database="CRICKET_DB",
        sql=f"""
            call LOAD_CRICKET_DATA()
        """,
    )

    # A sensor to verify if the files we read from raw s3 bucket are loaded to snowflake or not. This helps us verify both spark and snowflake tasks have performed as expected
    @task.sensor(poke_interval=30, timeout=300)
    def verify_snowflake_load(**context) -> PokeReturnValue:
        files_list = context['ti'].xcom_pull(task_ids='is_file_available')
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        files_str = ""
        for file in files_list:
            files_str += f"'{file}',"
        files_str =  files_str[0:len(files_str)-1]
        count = hook.get_first(f"SELECT COUNT(DISTINCT FILE_NAME) FROM CRICKET_DB.PUBLIC.MATCH WHERE FILE_NAME IN ({files_str})")
        print(count)
        print(len(files_list))

        # get_first returns a tuple with one value hence access that in index zero
        if count[0] == len(files_list):
            condition = True
        else:
            condition = False
        return PokeReturnValue(is_done=condition)
    
    # Once verification of load is done, delete files in raw and processed folder. Or is archival is needed, move them to another folder.
    @task
    def delete_files(**context):
        files_list = context['ti'].xcom_pull(task_ids='is_file_available')
        s3_hook=S3Hook(aws_conn_id='aws_s3_conn')
        s3_hook.delete_objects(bucket=s3_bucket_name, keys=files_list)

        keys_to_delete = list(s3_hook.list_keys(bucket_name=s3_bucket_name, prefix="processed_match_files/"))
        s3_hook.delete_objects(bucket=s3_bucket_name, keys=keys_to_delete)
    
    is_file_available() >> submit_spark_job >> snowflake_load >> verify_snowflake_load() >> delete_files()

cricket_data_analysis()