-- Databricks notebook source
select 
  driver_name, count(*) as total_races, 
  sum(calculated_points) as total_points,
  avg(calculated_points) as avg_points 
from f1_presentation.calculated_race_results
where race_year >= 2011
group by 1
having count(*) > 50
order by avg_points desc
