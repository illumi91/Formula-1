# Databricks notebook source
dbutils.widgets.text("sourcePath", "")
dbutils.widgets.text("fileDate", "2021-03-28")

data_source = dbutils.widgets.get("sourcePath")
file_date = dbutils.widgets.get("fileDate")
print(file_date)

# COMMAND ----------

# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

import sys
sys.path.append("/Workspace/Formula1/dependencies/pipeline")

# COMMAND ----------

# imports
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pipeline import pipeline

# COMMAND ----------

# DBTITLE 1,Ingest races.csv
database="f1_processed"
dataset="results"
schema = StructType(
    [
        StructField("constructorId", LongType(), True),
        StructField("driverId", LongType(), True),
        StructField("fastestLap", StringType(), True),
        StructField("fastestLapSpeed", StringType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("grid", LongType(), True),
        StructField("laps", LongType(), True),
        StructField("milliseconds", StringType(), True),
        StructField("number", StringType(), True),
        StructField("points", DoubleType(), True),
        StructField("position", StringType(), True),
        StructField("positionOrder", LongType(), True),
        StructField("positionText", StringType(), True),
        StructField("raceId", LongType(), True),
        StructField("rank", StringType(), True),
        StructField("resultId", LongType(), False),
        StructField("statusId", LongType(), True),
        StructField("time", StringType(), True),
    ]
)
column_rename_dict = {
    "resultId": "result_id",
    "raceId": "race_id",
    "driverId": "driver_id",
    "constructorId": "constructor_id",
    "positionText": "position_text",
    "positionOrder": "position_order",
    "fastestLap": "fastest_lap",
    "fastestLapTime": "fastest_lap_time",
    "fastestLapSpeed": "fastest_lap_speed",
    "time": "race_time",
}
columns_to_add = {
    "environment": data_source, 
    "file_date": file_date
}
columns_to_drop = ["statusId"]
partition_cols = ["race_id"]

df = pipeline(
    spark=spark,
    source_file_path=f"{raw_folder_path}/{file_date}/{dataset}.json",
    target_path=f"{processed_folder_path}/{dataset}",
    format="json",
    partition_cols=partition_cols,
    schema=schema,
    dataset=dataset,
    database=database,
    write_mode="append",
    column_rename_dict=column_rename_dict, 
    columns_to_drop=columns_to_drop,
    columns_to_add=columns_to_add,
)

# COMMAND ----------

target_path = f"{processed_folder_path}/{dataset}"
mode = "append"

for race_id in df.select("race_id").distinct().toLocalIterator():
    if (spark._jsparkSession.catalog().tableExists(f"{database}.{dataset}")):
        spark.sql(f"ALTER TABLE {database}.{dataset} DROP IF EXISTS PARTITION (race_id = '{race_id.race_id}')")
        print(f"Deleted partition {race_id.race_id}.")

df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable(f"{database}.{dataset}")


# COMMAND ----------

# MAGIC %sql select * from f1_processed.results

# COMMAND ----------

display(spark.sql(f"""
select file_date, race_id, count(*)
from {database}.`{dataset}`
group by 1, 2 order by 2 desc"""))

# COMMAND ----------



# COMMAND ----------

# database="f1_processed"
# dataset="results"

# spark.sql(f"""
# drop database {database} cascade""")

# COMMAND ----------

dbutils.notebook.exit("Success")
