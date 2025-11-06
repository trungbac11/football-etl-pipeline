{{ config(materialized='table') }}

select * 
from analysis.teamseason
where name='Manchester City' 
and date>='2015-08-08'
and date<='2016-05-16'