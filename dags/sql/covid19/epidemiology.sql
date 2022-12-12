insert into aws_raw_cosmic_energy.s3_to_snowflake_epidemiology
select *
    , current_timestamp() as etl_timestamp
from aws_stg_cosmic_energy.s3_to_snowflake_epidemiology
where key like 'US_IL_%'
and date::date between '2022-01-01'::date and '2022-03-31'::date