insert into aws_raw_cosmic_energy.s3_to_snowflake_index
select *
    , current_timestamp() as etl_timestamp
from aws_stg_cosmic_energy.s3_to_snowflake_index
where country_code = 'US';