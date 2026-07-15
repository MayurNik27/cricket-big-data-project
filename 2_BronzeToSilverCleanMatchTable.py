# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Importing Libraries
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Read bronze Table
bronze_df = spark.table('workspace.default.cricket_bronze_current_matches')

df_1=bronze_df.select('raw_json').collect()[0]['raw_json']
api_data=json.loads(df_1) #convert JSON string in to python dict

matches = api_data.get('data',[])

print("Total Matches found",len(matches))
print(matches[0] if len(matches)>0 else "No Matches")


# COMMAND ----------

# DBTITLE 1,Extract only the useful fields
silver_rows=[]

for match in matches:
    team=match.get('teams',[])
    score=match.get('score',[])
    
    team_1=team[0] if len(team)>0 else None
    team_2=team[1] if len(team)>0 else None

    score_1=None
    score_2=None

    if len(score)>0:
        s1 = score[0]
        #formating display of scores: 241/7 in 50 overs
        score_1=f"{s1.get("r")}/{s1.get("w")} in {s1.get("o")} overs"


    if len(score)>1:
        s2 = score[1]
        score_2=f"{s2.get("r")}/{s2.get("w")} in {s2.get("o")} overs"

    silver_rows.append({
        "match_id":match.get("id"),
        "match_name":match.get("name"),
        "status":match.get("status"),
        "venue":match.get("venue"),
        "match_date":match.get("date"),
        "date_time_gmt":match.get("dateTimeGMT"),
        "team_1":team_1,
        "team_2":team_2,
        "score_1":score_1,
        "score_2":score_2,
        "match_started":match.get("matchStarted"),
        "match_ended":match.get("matchEnded")
    })



print("Silver layers rows prepared",len(silver_rows))


# COMMAND ----------

silver_schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("match_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("match_date", StringType(), True),
    StructField("date_time_gmt", StringType(), True),
    StructField("team_1", StringType(), True),
    StructField("team_2", StringType(), True),
    StructField("score_1", StringType(), True),
    StructField("score_2", StringType(), True),
    StructField("match_started", BooleanType(), True),
    StructField("match_ended", BooleanType(), True)
])


silver_df=spark.createDataFrame(data=silver_rows,schema=silver_schema)\
    .withColumn("match_date",to_date(col("match_date"),"yyyy-mm-dd"))\
    .withColumn("loaded_at",current_timestamp())

display(silver_df)

# COMMAND ----------

silver_df.write\
    .format('delta')\
    .mode('overwrite')\
    .option('overwriteSchema',True)\
    .saveAsTable('workspace.default.cricket_silver_current_matches')


print("SILVER TABLE CREATED SUCCESSFYLLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.cricket_silver_current_matches
