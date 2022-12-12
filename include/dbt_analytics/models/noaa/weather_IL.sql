
with base as 
(select s.id, s.name, 
    to_date(date_recorded, 'YYYYMMDD') as date_recorded,
    case when element = 'SNOW' then value end as snowfall,
    case when element = 'TMIN' then value end as temp_min,
    case when element = 'TMAX' then value end as temp_max,
    case when element = 'PRCP' then value end as precipitation,
    case when element = 'WSFG' then value end as peak_wind_gust

from (
    select * from aws_raw_cosmic_energy.weather_elements 
    where year(to_date(date_recorded, 'YYYYMMDD')) = year(dateadd(day, -1, current_date()))
        and qflag IS NULL 
) we
join (
    select id, name
    from aws_raw_cosmic_energy.stations
    where state = 'IL'
) s
on we.id = s.id
where we.element in ('WSFG', 'TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD')
),

pivot_base
as 
(select 
 id, 
 name,
 date_recorded,
 max(snowfall) as snowfall,
 max(temp_min) as temp_min,
 max(temp_max) as temp_max,
 max(precipitation) as precipitation,
 max(peak_wind_gust) as peak_wind_gust
from base
group by id, date_recorded, name)

select id, name, date_trunc('month', date_recorded) as month,  monthname(date_recorded) as month_name, 
max(snowfall) as snowfall,
min(temp_min) as temp_min,
max(temp_max) as tem_max,
max(precipitation) as precipitation,
max(peak_wind_gust) as peak_wind_gust
from pivot_base
group by id, name, month, month_name
order by name, month
limit 10000