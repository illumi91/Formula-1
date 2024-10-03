-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

select *, concat(driver_ref, code) as new_driver_ref
from drivers

-- COMMAND ----------

select *, split(driver_name, " ")[0] forename, split(driver_name, " ")[1] surname
from drivers

-- COMMAND ----------

select current_timestamp(), *
from drivers

-- COMMAND ----------

select date_format(dob, 'dd-MM-yyyy'), *
from drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###AGGREGATE FUNCTIONS

-- COMMAND ----------

select count(*) from drivers;

-- COMMAND ----------

select max(dob) from drivers

-- COMMAND ----------

select nationality, count(*)
from drivers
group by 1
having count(*) > 100
order by 2 desc

-- COMMAND ----------

select rank() over(partition by nationality order by dob desc) as rank, *
from drivers
where nationality = 'British'
order by nationality, dob desc
