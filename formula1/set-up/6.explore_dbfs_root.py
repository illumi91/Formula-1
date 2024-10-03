# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore DBFS root
# MAGIC 1. List all the folders in the DBFS root
# MAGIC 2. Interact with DBFS File Browser
# MAGIC 3. Upload file to DBFS root

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------

df_circuits = spark.read.format("csv").options(header="true").load("/FileStore/circuits.csv")
display(df_circuits)
