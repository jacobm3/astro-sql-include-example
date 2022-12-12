insert into aws_raw_cosmic_energy.s3_to_snowflake_vaccinations
select DATE
    , KEY
    , NEW_PERSONS_VACCINATED
    , CUMULATIVE_PERSONS_VACCINATED
    , NEW_PERSONS_FULLY_VACCINATED
    , CUMULATIVE_PERSONS_FULLY_VACCINATED
    , NEW_VACCINE_DOSES_ADMINISTERED
    , CUMULATIVE_VACCINE_DOSES_ADMINISTERED
    , current_timestamp() as etl_timestamp
from aws_stg_cosmic_energy.s3_to_snowflake_vaccinations
where key like 'US_IL_%'
    and date::date between '2022-01-01'::date and '2022-03-31'::date