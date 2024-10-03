# Databricks notebook source
dbutils.widgets.text("fileDate", "2021-03-21")

file_date = dbutils.widgets.get("fileDate")

# COMMAND ----------

# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

# MAGIC %run "../dependencies/includes/functions"

# COMMAND ----------

from pyspark.sql.functions import col

race_years_to_process_list = df_column_to_list(spark.read.parquet(f"{presentation_folder_path}/race_results"), "race_year")

df_race_results = spark.read.parquet(f"{presentation_folder_path}/race_results") \
                                .filter(col("race_year").isin(race_years_to_process_list))
                                

# COMMAND ----------

from pyspark.sql.functions import sum, when, desc, rank, lead
from pyspark.sql.window import Window

df_race_results_groupby = df_race_results.groupBy("race_year", "nationality", "driver_name", "constructor_name") \
                                                    .agg(sum(when(df_race_results.position == 1, 1).otherwise(0)).alias("wins"), 
                                                            sum("points").alias("total_points"))
                                                        
window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
driver_standings_df = df_race_results_groupby.withColumn("rank", rank().over(window_spec)) \
                                                .orderBy(desc("race_year"), desc("total_points")) \
                                                    .select([
                                                            'rank',
                                                            'race_year',
                                                            'nationality',
                                                            'driver_name',
                                                            'constructor_name',
                                                            'wins',
                                                            'total_points',
                                                            ])

display(driver_standings_df)

# COMMAND ----------

overwrite_partition(driver_standings_df, "f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings
