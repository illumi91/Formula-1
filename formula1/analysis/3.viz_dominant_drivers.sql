-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Aril">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

with v_dominant_drivers as (
select 
  driver_name, count(*) as total_races, 
  sum(calculated_points) as total_points,
  round(avg(calculated_points), 2) as avg_points,
  rank() over(order by avg(calculated_points) desc) as rank
from f1_presentation.calculated_race_results
group by 1
having count(*) > 50
order by avg_points desc
)
select 
  race_year,
  driver_name, 
  count(*) as total_races, 
  sum(calculated_points) as total_points,
  round(avg(calculated_points), 2) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where rank <= 10)
group by 1, 2
order by race_year desc, avg_points desc

-- COMMAND ----------

select 
  race_year,
  driver_name, 
  count(*) as total_races, 
  sum(calculated_points) as total_points,
  round(avg(calculated_points), 2) as avg_points
from f1_presentation.calculated_race_results
where driver_name in (select driver_name from v_dominant_drivers where rank <= 10)
group by 1, 2
order by race_year desc, avg_points desc
