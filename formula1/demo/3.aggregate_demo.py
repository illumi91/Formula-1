# Databricks notebook source
# MAGIC %run "../dependencies/configuration/environment"

# COMMAND ----------

df_race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------


demo_df = df_race_results.filter("race_year = 2020")
display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

display(demo_df.select(count("stop")))

# COMMAND ----------

display(demo_df.select(countDistinct("race_name")))

# COMMAND ----------

display(demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")))

# COMMAND ----------

display(demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")))

# COMMAND ----------

groupby_df = demo_df.groupBy("driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name").alias("races_completed"))
        
display(groupby_df.orderBy(groupby_df.total_points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

demo_df = df_race_results.filter("race_year in (2019, 2020)")

# COMMAND ----------

# from pyspark.sql.functions import *

display(demo_df.select("race_year").distinct().orderBy(demo_df.race_year.desc()))

# COMMAND ----------

demo_df.groupBy("race_year", "driver_name").agg(sum("points"), countDistinct("race_name"))

# COMMAND ----------

from pyspark.sql.functions import asc, desc, sum, countDistinct

demo_grouped_df = demo_df.groupBy("race_year", "driver_name") \
        .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
        .orderBy(asc("race_year"), desc(sum("points")))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))

display(demo_grouped_df.withColumn("rank", rank().over(window_spec)))
