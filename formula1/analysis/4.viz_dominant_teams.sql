-- Databricks notebook source
create or replace temp view v_dominant_constructors
as
select 
  constructor_name, count(*) as total_races, 
  sum(calculated_points) as total_points,
  round(avg(calculated_points), 2) as avg_points,
  rank() over(order by avg(calculated_points) desc) as rank
from f1_presentation.calculated_race_results
group by 1
having count(*) > 100
order by avg_points desc

-- COMMAND ----------

select 
  race_year,
  constructor_name, 
  count(*) as total_races, 
  sum(calculated_points) as total_points,
  round(avg(calculated_points), 2) as avg_points
from f1_presentation.calculated_race_results
where constructor_name in (select constructor_name from v_dominant_constructors where rank <= 5)
group by 1, 2
order by race_year desc, avg_points desc

-- COMMAND ----------

select 
  race_year,
  constructor_name, 
  count(*) as total_races, 
  sum(calculated_points) as total_points,
  round(avg(calculated_points), 2) as avg_points
from f1_presentation.calculated_race_results
where constructor_name in (select constructor_name from v_dominant_constructors where rank <= 10)
group by 1, 2
order by race_year desc, avg_points desc
