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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pipeline import pipeline

# COMMAND ----------

# DBTITLE 1,Ingest circuit.csv
database="f1_processed"
dataset="circuits"
load_options_dict = {
    "header": True,
}
schema = StructType(
    [
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", IntegerType(), True),
        StructField("url", StringType(), True),
    ]
)
column_rename_dict = {
    "circuitId": "circuit_id",
    "circuitRef": "circuit_ref",
    "lat": "latitude",
    "long": "longitude",
    "alt": "altitude",
    "name": "circuit_name",
}
columns_to_add = {
    "environment": data_source, 
    "file_date": file_date
}
columns_to_drop = ["url"]

df = pipeline(
    spark=spark,
    source_file_path=f"{raw_folder_path}/{file_date}/{dataset}.csv",
    target_path=f"{processed_folder_path}/{dataset}",
    format="csv",
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

# MAGIC %sql select file_date, * from f1_processed.circuits

# COMMAND ----------

display(spark.sql(f"""
select file_date, count(*)
from {database}.{dataset}
group by 1
order by 1
"""))

# COMMAND ----------

dbutils.notebook.exit("Success")
