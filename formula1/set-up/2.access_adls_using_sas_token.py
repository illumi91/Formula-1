# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

sas_token = dbutils.secrets.get(scope="formula1-scope", key="sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1databrickscourself.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1databrickscourself.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1databrickscourself.dfs.core.windows.net", sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1databrickscourself.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv("abfss://demo@f1databrickscourself.dfs.core.windows.net/circuits.csv", header=True)
display(df)
