# Databricks notebook source
dbutils.widgets.text("sourcePath", "")
dbutils.widgets.text("fileDate", "2021-03-21")

data_source = dbutils.widgets.get("sourcePath")
file_date = dbutils.widgets.get("fileDate")

# COMMAND ----------

# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

import sys
sys.path.append("/Workspace/Formula1/dependencies/pipeline")

# COMMAND ----------

# imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pipeline import pipeline

# COMMAND ----------

# DBTITLE 1,Ingest races.csv
database="f1_processed"
dataset="races"
load_options_dict = {
    "header": "True",
}
schema = StructType(
    [
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)
column_rename_dict = {
    "raceId": "race_id",
    "year": "race_year",
    "circuitId": "circuit_id",
    "name": "race_name",
    "date": "race_date",
}
columns_to_add = {
    "environment": data_source, 
    "file_date": file_date
}
columns_to_drop = ["date", "time", "url"]
partition_cols = ["race_year"]

df = pipeline(
    spark=spark,
    source_file_path=f"{raw_folder_path}/{file_date}/{dataset}.csv",
    target_path=f"{processed_folder_path}/{dataset}",
    format="csv",
    concat_date_and_time=True,
    partition_cols=partition_cols,
    schema=schema,
    dataset=dataset,
    database=database,
    write_mode="overwrite",
    load_options_dict=load_options_dict, 
    column_rename_dict=column_rename_dict, 
    columns_to_drop=columns_to_drop,
    columns_to_add=columns_to_add,
)

# COMMAND ----------

display(spark.sql(f"""
select file_date, count(*)
from {database}.{dataset}
group by 1
order by 1
"""))

# COMMAND ----------

dbutils.notebook.exit("Success")
