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
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pipeline import pipeline

# COMMAND ----------

# DBTITLE 1,Ingest races.csv
database="f1_processed"
dataset="pit_stops"
load_options_dict = {"multiline": True}
schema = StructType(
    [
        StructField("driverId", LongType(), True),
        StructField("duration", StringType(), True),
        StructField("lap", LongType(), True),
        StructField("milliseconds", LongType(), True),
        StructField("raceId", LongType(), True),
        StructField("stop", LongType(), True),
        StructField("time", StringType(), True),
    ]
)
column_rename_dict = {
    "raceId": "race_id",
    "driverId": "driver_id",
}
columns_to_add = {
    "environment": data_source, 
    "file_date": file_date
}
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
    load_options_dict=load_options_dict, 
    column_rename_dict=column_rename_dict,
    columns_to_add=columns_to_add,
)

# COMMAND ----------

display(spark.sql(f"""
select file_date, race_id, count(*)
from {database}.{dataset}
group by 1, 2 order by 2"""))

# COMMAND ----------

# %sql drop table f1_processed.pit_stops

# COMMAND ----------

dbutils.notebook.exit("Success")
