# Databricks notebook source
# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

df_filtered = df.filter((df["race_year"] == 2019) & (df["round"] <= 5))
display(df_filtered)
