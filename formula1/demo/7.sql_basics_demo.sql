-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from f1_processed.drivers

-- COMMAND ----------

desc drivers

-- COMMAND ----------

select * from f1_processed.drivers limit 10

-- COMMAND ----------

select nationality, count(*) 
from drivers 
group by 1
order by 2 desc
