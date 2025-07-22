# Cricket-DataPipeline-using-Spark-Airflow
This Repository contains the code to load cricket data on an incremental basis to snowflake tables where the transformations are handled by Spark and orchestration done by Airflow.

## Files Information:

- SampleData: Contains JSON files containing ball by ball data of a cricket match. One file represents one match. Data Sourced from - https://cricsheet.org/
- Pyspark-cluster-with-jupyter: Contains docker compose file to setup a spark cluster locally on docker.
- pyspark-jupyter-lab-old: Contains Dockerfile used to build the base image for the spark cluster.
- Snowflake_objects.sql: Contains snowflake scripts to create tables and procedures required to load and store cricket match and innings data.
- airflow-docker-compose.yaml: docker compose file to run an airflow cluster locally on docker.
- cricket_data_analysis.py: Airflow Dag file containing tasks to run the entire pipeline.
- cricketdataanalysis.py: Pyspark code to read and transform the raw cricket match files.
- dockerfile: Dockerfile used to build base image for the airflow cluster.


## Project Flow:

### Check for the availabe files in the s3 bucket

The match json files are placed in this bucket on a daily basis before the airflow dag is triggered. We need to check if there are any files present in the bucket and also wait for 5 minutes (using airflow sensors) for the files to be placed. Once the files are available, we get the metadata of the files and store them in a .txt file in the same bucket for downstream processes to access.

### Transform JSON files using Spark

The data present in JSON needs to be processed and transformed into a more suitable format like parquet which is efficient in storage and also faster while it is retrieved by snowflake. To process the files, a spark job will be submitted to the spark cluster that is running locally in seperate containers. The spark job will process the json file and load them as optimized parquet into a seperate processed folder in the bucket.
Note: For airflow that is running in a container to connect to spark running in another container, these containers needs to be present in same docker network. Hence, create a docker network and add them in the docker compose files for both of them.

### Load Parquet to Snowflake

Snowflake reads the parquet files present in the processed folders and loads them to tables. The data is first loaded into a raw table in a single variant column. Then the data is segregated, flattened and loaded to a much more structured table. This is done through snowflake procedures written in Python.

### Verify Snowflake Load

To verify if the spark job and snowflake procedure has executed as expected and loaded all the files that were present in the bucket, a sensor task is run in airflow which checks if the initial list of files that were read from the bucket is present in the snowflake tables or not.

### Delete files

Once the files are processed and loaded to target warehouse, the folders needs to be cleaned up for the next set of loads.
