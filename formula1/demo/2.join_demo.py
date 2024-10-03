# Databricks notebook source
# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

df_circuits = spark.read.parquet(f"{processed_folder_path}/circuits").filter("circuit_id < 70")
df_races = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

display(df_circuits)

# COMMAND ----------

# MAGIC %md
# MAGIC Inner join

# COMMAND ----------

races_circuits_df = df_races.join(
                                df_circuits, 
                                df_races.circuit_id == df_circuits.circuit_id, 
                                "inner"
                                ) \
                                .select(
                                    df_circuits.circuit_name, 
                                    df_circuits.location, 
                                    df_circuits.country, 
                                    df_races.race_name, 
                                    df_races.round
                                )
display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Outer Join

# COMMAND ----------

# Left Outer Join
from pyspark.sql.functions import asc_nulls_last

circuits_races_df = df_circuits.join(
                                df_races, 
                                df_circuits.circuit_id == df_races.circuit_id, 
                                "left"
                                ) \
                                .select(
                                    df_circuits.circuit_id, 
                                    df_circuits.circuit_name, 
                                    df_circuits.location, 
                                    df_circuits.country, 
                                    df_races.race_name, 
                                    df_races.round
                                ) \
                                .orderBy(asc_nulls_last(df_races.round))
display(circuits_races_df)

# COMMAND ----------

# Right Outer Join
from pyspark.sql.functions import asc_nulls_last

circuits_races_df = df_circuits.join(
                                df_races, 
                                df_circuits.circuit_id == df_races.circuit_id, 
                                "right"
                                ) \
                                .select(
                                    df_circuits.circuit_id, 
                                    df_circuits.circuit_name, 
                                    df_circuits.location, 
                                    df_circuits.country, 
                                    df_races.race_name, 
                                    df_races.round
                                ) \
                                .orderBy(asc_nulls_last(df_races.round))
display(circuits_races_df)

# COMMAND ----------

# Full Outer Join
from pyspark.sql.functions import asc_nulls_last

circuits_races_df = df_circuits.join(
                                df_races, 
                                df_circuits.circuit_id == df_races.circuit_id, 
                                "full"
                                ) \
                                .select(
                                    df_circuits.circuit_id, 
                                    df_circuits.circuit_name, 
                                    df_circuits.location, 
                                    df_circuits.country, 
                                    df_races.race_name, 
                                    df_races.round
                                ) \
                                .orderBy(asc_nulls_last(df_races.round))
display(circuits_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Semi Join
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import asc_nulls_last

circuits_races_df = df_circuits.join(
                                df_races, 
                                df_circuits.circuit_id == df_races.circuit_id, 
                                "semi"
                                ) \
                                .select(
                                    df_circuits.circuit_id, 
                                    df_circuits.circuit_name, 
                                    df_circuits.location, 
                                    df_circuits.country
                                )
display(circuits_races_df)

# COMMAND ----------

# MAGIC %md Anti Join

# COMMAND ----------

circuits_races_df = df_races.join(
                            df_circuits, 
                            df_circuits.circuit_id == df_races.circuit_id, 
                            "anti"
                            )
display(circuits_races_df)

# COMMAND ----------

# MAGIC %md Cross Join

# COMMAND ----------

races_circuits_df = df_races.crossJoin(df_circuits)
display(races_circuits_df)
