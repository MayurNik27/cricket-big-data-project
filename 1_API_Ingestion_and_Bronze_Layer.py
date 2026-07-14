# Databricks notebook source
# DBTITLE 1,Import the required library
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create Catalog, Schema, Volume
spark.sql("CREATE CATALOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")


base_path = '/Volumes/workspace/default/cricket_api_project/'

# COMMAND ----------

# DBTITLE 1,CALLING Cricket API
API_KEY = '9f5f6add-83ea-4096-b5b6-a9c5b2d73fd8'
api_url=f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"

response=requests.get(api_url)
response.raise_for_status()

api_data=response.json()
print(api_data.keys())

print(json.dumps(api_data,indent=2)[:2000])

# COMMAND ----------

# DBTITLE 1,Save the RAW API response in the Volume
raw_file_path=f"{base_path}/current_matches_raw.json"

with open(raw_file_path,'w') as file:
    json.dump(api_data,file)

print(f"Raw API File Data is saved at the: {raw_file_path}")

# COMMAND ----------

# DBTITLE 1,Create Bronze Layer Dataframe and Table
brozne_data=[{
    "source_api":api_url,
    "raw_json":json.dumps(api_data),
    "ingestion_time":None
}]

bronze_schema=StructType([
    StructField("source_api",StringType(),True),
    StructField("raw_json",StringType(),True),
    StructField("ingestion_time",TimestampType(),True)
])

bronze_df=spark.createDataFrame(data=brozne_data,schema=bronze_schema)\
    .withColumn("ingestion_time",current_timestamp())

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Save the Bronze Layer DataFrame in Table
bronze_df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("workspace.default.cricket_bronze_current_matches")

print("BRONZE LAYER TABLE CREATED SUCCESSFULLY")
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.cricket_bronze_current_matches
