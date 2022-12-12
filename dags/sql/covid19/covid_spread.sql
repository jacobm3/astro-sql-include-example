create or replace table aws_cosmic_energy.snowflake_op_spread
as
select 
    idx.region_name, idx.zip_code
    , total_population
    , elderly_population
    , kids_population
    , confirmed
    , deceased
    , (confirmed/total_population)*100 as confirned_pct
    , (deceased/total_population)*100 as deceased_pct
    , current_timestamp as etl_timestamp
from 
(select  
    subregion2_name as region_name
    , subregion2_code as zip_code 
from aws_raw_cosmic_energy.S3_TO_SNOWFLAKE_index
where country_code = 'US'
    and key like 'US_IL%'
) idx

left join 
(select 
    substring(key, 7,5) as zip_code
    , population as total_population
    , (population_age_60_69 + population_age_70_79 + population_age_80_and_older) as elderly_population
    , population_age_10_19 as kids_population
from aws_raw_cosmic_energy.S3_TO_SNOWFLAKE_demographics
where key like 'US_IL_%'
) dmg
on idx.zip_code = dmg.zip_code

left join 
(
select 
    substring(key, 7,5) as zip_code
    , max(cumulative_confirmed) as confirmed
    , max(cumulative_deceased) as deceased
from aws_raw_cosmic_energy.S3_TO_SNOWFLAKE_epidemiology
where key like 'US_IL_%'
    and date_part(year, date::date) = '2022' and date_part(month, date::date) in (1,2,3)
group by zip_code
) epd
on idx.zip_code = epd.zip_code
order by zip_code
limit 10000;