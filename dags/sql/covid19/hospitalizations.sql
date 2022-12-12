insert into awS_raw_cosmic_energy.covid_hospitalizations
select *
    , current_timestamp() as etl_timestamp
from aws_stg_cosmic_energy.covid_hospitalizations
where key like 'US_IL_%'
    and date::date between '2022-01-01'::date and '2022-03-31'::date