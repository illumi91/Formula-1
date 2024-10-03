# Databricks notebook source
# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation;

# COMMAND ----------

for file in dbutils.fs.ls(presentation_folder_path):
    dataset = file.name.split("/")[0]
    location = f"{presentation_folder_path}/{dataset}/"
    spark.sql(f"DROP TABLE IF EXISTS f1_presentation.{dataset};")
    spark.sql(f"""
        CREATE TABLE f1_presentation.{dataset}
        USING parquet
        LOCATION '{location}';
    """)
    print(f"Processed: {dataset}.")    


# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in f1_presentation

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in f1_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in f1_processed

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_raw.circuits

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.circuits
