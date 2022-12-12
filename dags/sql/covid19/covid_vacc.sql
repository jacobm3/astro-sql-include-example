create or replace table aws_cosmic_energy.snowflake_op_vacc_vs_hosp
as
select 
    epd.record_date
    , epd.zip_code
    , epd.new_confirmed
    , epd.new_deceased
    , vcc.cumulative_persons_fully_vaccinated
    , vcc.new_persons_fully_vaccinated
from 
(select 
    date::date as record_date
    , substring(key, 7,5) as zip_code
    , new_confirmed
    , new_deceased
from aws_raw_cosmic_energy.S3_TO_SNOWFLAKE_epidemiology
where key like 'US_IL_%'
    and date_part(year, date::date) = '2022' and date_part(month, date::date) in (2)
) epd
left join 

(select 
    date::date as record_date
    , substring(key, 7,5) as zip_code
    , new_persons_fully_vaccinated
    , cumulative_persons_fully_vaccinated
from aws_raw_cosmic_energy.S3_TO_SNOWFLAKE_vaccinations
where key like 'US_IL_%'
    and date_part(year, date::date) = '2022' and date_part(month, date::date) in (2)
) vcc
on epd.record_date = vcc.record_date
    and epd.zip_code = vcc.zip_code
limit 10000