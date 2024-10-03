# Databricks notebook source
# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

df_race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

df_race_results.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

df_race_results_2020 = spark.sql("""select * 
from v_race_results
where race_year = 2020""")

display(df_race_results_2020)
