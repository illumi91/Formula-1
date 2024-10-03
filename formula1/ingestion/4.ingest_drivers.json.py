# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

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
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pipeline import pipeline

# COMMAND ----------

# DBTITLE 1,Ingest races.csv
database="f1_processed"
dataset="drivers"
name_schema = StructType(
    [
        StructField("forename", StringType(), False),
        StructField("surname", StringType(), False),
    ]
)
schema = StructType(
    [
        StructField("code", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("driverId", LongType(), True),
        StructField("driverRef", StringType(), False),
        StructField("name", name_schema),
        StructField("nationality", StringType(), False),
        StructField("number", StringType(), False),
        StructField("url", StringType(), False),
    ]
)
column_rename_dict = {
    "driverId": "driver_id",
    "driverRef": "driver_ref",
    "name": "driver_name",
}
columns_to_add = {
    "environment": data_source, 
    "file_date": file_date
}
columns_to_drop = ["url"]
concat_column_name = "name"
columns_to_concat = ["name.forename", "name.surname"] 


pipeline(
    spark=spark,
    source_file_path=f"{raw_folder_path}/{file_date}/{dataset}.json",
    target_path=f"{processed_folder_path}/{dataset}",
    format="json",
    concat_column_name=concat_column_name,
    columns_to_concat=columns_to_concat,
    schema=schema,
    dataset=dataset,
    database=database,
    write_mode="overwrite",
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
