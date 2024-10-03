# Databricks notebook source
dbutils.widgets.text("fileDate", "2021-03-21")

file_date = dbutils.widgets.get("fileDate")

# COMMAND ----------

# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

# MAGIC %run "../dependencies/includes/functions"

# COMMAND ----------

# Load datasets
df_circuits = spark.read.parquet(f"{processed_folder_path}/circuits")
df_races = spark.read.parquet(f"{processed_folder_path}/races")
df_drivers = spark.read.parquet(f"{processed_folder_path}/drivers")
df_results = spark.read.parquet(f"{processed_folder_path}/results") \
                    .filter(f"file_date = '{file_date}'")
df_pit_stops = spark.read.parquet(f"{processed_folder_path}/pit_stops")
df_constructors = spark.read.parquet(f"{processed_folder_path}/constructors")
df_qualifying = spark.read.parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

races_circuits_df = df_races.join(df_circuits, df_races.circuit_id == df_circuits.circuit_id, "left") \
                                .select(
                                        df_races.race_id,
                                        df_races.race_year,
                                        df_races.race_name,
                                        df_races.race_date,
                                        df_circuits.circuit_name,
                                )

# df_pit_stops = df_pit_stops.groupBy("race_id", "driver_id").max("stop").withColumnRenamed("max(stop)", "stop")                                

race_results_df = df_results.join(df_drivers, df_drivers.driver_id == df_results.driver_id, "left") \
                                .join(races_circuits_df, df_results.race_id == races_circuits_df.race_id, "left") \
                                .join(df_constructors, df_results.constructor_id == df_constructors.constructor_id, "left") \
                                # .join(df_pit_stops, (df_races.race_id == df_pit_stops.race_id) & 
                                #                         (df_drivers.driver_id == df_pit_stops.driver_id), "left") \
                                # .filter((df_races["race_year"] == 2020) & (df_races["race_name"] == "Abu Dhabi Grand Prix"))
                                
race_results_df = race_results_df.select(
        races_circuits_df.race_id,
        races_circuits_df.circuit_name,
        races_circuits_df.race_name,
        races_circuits_df.race_date,
        races_circuits_df.race_year,
        df_drivers.driver_name,
        df_drivers.number,
        df_drivers.nationality,
        df_constructors.constructor_name,
        df_results.grid,
        df_results.position,
        # df_pit_stops.stop,
        df_results.fastest_lap_time,
        df_results.race_time,
        df_results.points,
        df_results.file_date,
        lit(current_timestamp().alias("current_timespamp"))
)

# COMMAND ----------

overwrite_partition(race_results_df, "f1_presentation", "race_results", "race_id")
