-- Databricks notebook source


-- COMMAND ----------

create or replace temp view v_driver_standings_2018
as
select race_year, driver_name, constructor_name, total_points, wins, rank
from f1_presentation.driver_standings
where race_year = 2018

-- COMMAND ----------

create or replace temp view v_driver_standings_2020
as
select race_year, driver_name, constructor_name, total_points, wins, rank
from f1_presentation.driver_standings
where race_year = 2020

-- COMMAND ----------

select * from v_driver_standings_2018

-- COMMAND ----------

select * from v_driver_standings_2020

-- COMMAND ----------

select * 
from v_driver_standings_2018 d_2018 
inner join v_driver_standings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

select * 
from v_driver_standings_2018 d_2018 
full outer join v_driver_standings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

select * 
from v_driver_standings_2018 d_2018 
semi join v_driver_standings_2020 d_2020 on d_2018.driver_name = d_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Anti Join

-- COMMAND ----------

select *
from v_driver_standings_2018 as d_2018
left anti join  v_driver_standings_2020 as d_2020 on d_2018.driver_name = d_2020.driver_name 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Cross Join

-- COMMAND ----------

select *
from v_driver_standings_2018 as d_2018
cross join v_driver_standings_2020 as d_2020
