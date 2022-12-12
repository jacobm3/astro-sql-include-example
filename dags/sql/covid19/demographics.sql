insert into aws_raw_cosmic_energy.s3_to_snowflake_demographics
select *
    , current_timestamp() as etl_timestamp
from aws_stg_cosmic_energy.s3_to_snowflake_demographics
-- where key like 'US_\_\__%'
where key like 'US_IL_%'