# Databricks notebook source
# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed;

# COMMAND ----------

for file in dbutils.fs.ls(processed_folder_path):
    dataset = file.name.split("/")[0]
    if dataset != "results":
        location = f"{processed_folder_path}/{dataset}"
        spark.sql(f"DROP TABLE IF EXISTS f1_processed.{dataset};")
        spark.sql(f"""
            CREATE TABLE f1_processed.{dataset}
            USING parquet
            LOCATION '{location}';
        """)
        print(f"Processed: {dataset}.")


# COMMAND ----------

for file in dbutils.fs.ls(processed_folder_path):
    dataset = file.name.split("/")[0]
    if dataset not in ["races", "results"]:
        location = f"{processed_folder_path}/{dataset}"
        spark.sql(f"DROP TABLE IF EXISTS f1_processed.{dataset};")
        spark.sql(f"""
            CREATE TABLE f1_processed.{dataset}
            USING parquet
            LOCATION '{location}';
        """)
        print(f"Processed: {dataset}.")


# COMMAND ----------

df = spark.read.parquet("/mnt/f1databrickscourself/processed/races")
df.printSchema()

# COMMAND ----------

race_id INT,
round INT,
circuit_id INT,
race_name STRING,
race_date DATE,
race_timestamp TIMESTAMP,
ingestion_date TIMESTAMP,
data_sorce STRING,
race_year INT

# COMMAND ----------

location = "/mnt/f1databrickscourself/processed/results"

df = spark.read.parquet(location)
display(df)

# COMMAND ----------

location = "/mnt/f1databrickscourself/processed/results"

spark.sql(f"DROP TABLE IF EXISTS f1_processed.results;")
spark.sql(f"""
    CREATE TABLE f1_processed.results (
        constructor_id LONG,
        driver_id LONG,
        fastest_lap STRING,
        fastest_lap_speed STRING,
        fastest_lap_time STRING,
        grid LONG,
        laps LONG,
        milliseconds STRING,
        number STRING,
        points DOUBLE,
        position STRING,
        position_order LONG,
        position_text STRING,
        rank STRING,
        result_id LONG,
        race_time STRING,
        ingestion_date TIMESTAMP,
        data_sorce STRING
    )
    USING parquet
    PARTITIONED BY (race_id INT)
    OPTIONS (path = '{location}');
""")
print(f"Processed: results.")

# COMMAND ----------

# MAGIC %sql msck repair table f1_processed.races

# COMMAND ----------

location = "/mnt/f1databrickscourself/processed/races"

spark.sql(f"DROP TABLE IF EXISTS f1_processed.races;")
spark.sql(f"""
    CREATE TABLE f1_processed.races (
        race_id INT,
        round INT,
        circuit_id INT,
        race_name STRING,
        race_date DATE,
        race_timestamp TIMESTAMP,
        ingestion_date TIMESTAMP,
        data_sorce STRING
    )
    USING parquet
    PARTITIONED BY (race_year INT)
    LOCATION '{location}';
""")
print(f"Processed: races.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.circuits

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits
