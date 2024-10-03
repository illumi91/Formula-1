# Databricks notebook source
dbutils.widgets.text("sourcePath", "")
dbutils.widgets.text("fileDate", "2021-04-28")

data_source = dbutils.widgets.get("sourcePath")
file_date = dbutils.widgets.get("fileDate")

# COMMAND ----------

# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

import sys
sys.path.append("/Workspace/Formula1/dependencies/pipeline")

# COMMAND ----------

# imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pipeline import pipeline

# COMMAND ----------

# DBTITLE 1,Ingest races.csv
database="f1_processed"
dataset="lap_times"
schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("lap", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
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
    source_file_path=f"{raw_folder_path}/{file_date}/{dataset}",
    target_path=f"{processed_folder_path}/{dataset}",
    format="csv",
    partition_cols=partition_cols,
    schema=schema,
    dataset=dataset,
    database=database,
    write_mode="append",
    column_rename_dict=column_rename_dict,
    columns_to_add=columns_to_add,
)

# COMMAND ----------

display(spark.sql(f"""
select file_date, race_id, count(*)
from {database}.{dataset}
group by 1, 2 order by 2 desc"""))

# COMMAND ----------

dbutils.notebook.exit("Success")
