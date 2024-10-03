-- Databricks notebook source

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables

-- COMMAND ----------

use database demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md MANAGED TABLES

-- COMMAND ----------

-- MAGIC %run "../dependencies/configuration/environment"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df_race_results.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables

-- COMMAND ----------

describe table extended race_results_python

-- COMMAND ----------

select * from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

create table demo.race_results_sql
as
select * 
from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

select * from demo.race_results_sql

-- COMMAND ----------

describe extended demo.race_results_sql

-- COMMAND ----------

select current_database()


-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df_race_results.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

describe  demo.race_results_python

-- COMMAND ----------

-- MAGIC %py 
-- MAGIC df = spark.sql("describe  demo.race_results_python")
-- MAGIC

-- COMMAND ----------

-- MAGIC %py
-- MAGIC for row in df.toLocalIterator():
-- MAGIC     print(f"{row.col_name} {str(row.data_type).upper()},")

-- COMMAND ----------

describe extended demo.race_results_ext_py

-- COMMAND ----------


select current_database()

-- COMMAND ----------

create table demo.race_results_ext_sql
(race_id INT,
circuit_name STRING,
race_name STRING,
race_date DATE,
race_year INT,
driver_name STRING,
number STRING,
nationality STRING,
constructor_name STRING,
grid BIGINT,
position STRING,
fastest_lap_time STRING,
race_time STRING,
points DOUBLE,
current_timespamp TIMESTAMP
)
using parquet
location "/mnt/f1databrickscourself/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

select * from demo.race_results_ext_sql

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

create or replace temp view race_results_2020 as select * from demo.race_results_python where race_year = 2020

-- COMMAND ----------

show views

-- COMMAND ----------

select * from race_results_2020
