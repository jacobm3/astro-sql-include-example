create or replace table aws_cosmic_energy.weather_stations
as
select 
    we.id, we.element, we.value, we.mflag, we.qflag, we.sflag, we.time_recorded, 
    i.latitude, i.longitude, 
    s.elevation
from aws_raw_cosmic_energy.weather_elements we
join aws_raw_cosmic_energy.inventory i
    on we.id = i.id
        and we.element = i.element
join aws_raw_cosmic_energy.stations s
    on we.id = s.id
where we.date_recorded = '20221025'
order by we.id 
limit 1000