# Databricks notebook source
result_status = dbutils.notebook.run("1.ingest_circuits.csv", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})
print(result_status)
result_status = dbutils.notebook.run("2.ingest_races.csv", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})
print(result_status)
result_status = dbutils.notebook.run("3.ingest_constructors.json", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})
print(result_status)
result_status = dbutils.notebook.run("4.ingest_drivers.json", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})
print(result_status)
result_status = dbutils.notebook.run("5.ingest_results.json", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})
print(result_status)
result_status = dbutils.notebook.run("6.ingest_pit_stops.json", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})
print(result_status)
result_status = dbutils.notebook.run("7.ingest_lap_times.csv", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})
print(result_status)
result_status = dbutils.notebook.run("8.ingest_qualifying.json", 0, {"data_source": "Ergast API", "file_date": "2021-03-21"})
print(result_status)

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*)
# MAGIC from f1_processed.results
# MAGIC group by 1
# MAGIC order by 1 desc
