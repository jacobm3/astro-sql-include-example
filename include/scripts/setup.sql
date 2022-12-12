create schema aws_cosmic_energy;

create schema aws_stg_cosmic_energy;

create schema aws_raw_cosmic_energy;

create or replace stage aws_stg_cosmic_energy.s3_covid
storage_integration=s3_sandbox_int
url='s3://astronomer-field-engineering-demo/incoming/covid19/'
file_format=(TYPE=CSV, field_delimiter = ',');

create or replace stage aws_stg_cosmic_energy.s3_noaa
storage_integration=s3_sandbox_int
url='s3://astronomer-field-engineering-demo/noaa-ghcnd/'
file_format=(TYPE=CSV, field_delimiter = '|');

create table aws_stg_cosmic_energy.stations
(
    record string
);


create or replace table aws_raw_cosmic_energy.stations
(id string
 , latitude float
 , longitude float
 , elevation float
 , state string
 , name string
 , gsn_flag string
 , hcn_crn_flag string
 , wmo_id string
 , etl_timestamp timestamp
);

create or replace TABLE aws_stg_cosmic_energy.WEATHER_ELEMENTS 
( 
    ID VARCHAR(16777216)
    , DATE_RECORDED VARCHAR(16777216)
    , ELEMENT VARCHAR(16777216)
    , VALUE VARCHAR(16777216)
    , MFLAG VARCHAR(16777216)
    , QFLAG VARCHAR(16777216)
    , SFLAG VARCHAR(16777216)
    , TIME_RECORDED VARCHAR(16777216) 
);

create or replace TABLE aws_raw_cosmic_energy.WEATHER_ELEMENTS 
( 
    ID VARCHAR(16777216)
    , DATE_RECORDED VARCHAR(16777216)
    , ELEMENT VARCHAR(16777216)
    , VALUE VARCHAR(16777216)
    , MFLAG VARCHAR(16777216)
    , QFLAG VARCHAR(16777216)
    , SFLAG VARCHAR(16777216)
    , TIME_RECORDED VARCHAR(16777216) 
);

create or replace table STG_COSMIC_ENERGY.covid_epidemiology
(
date string
, key string
, new_confirmed integer
, new_deceased integer
, new_recovered integer
, new_tested integer
, cumulative_confirmed integer
, cumulative_deceased integer
, cumulative_recovered integer
, cumulative_tested integer
);


create or replace table stg_cosmic_energy.covid_index
(
location_key string
, place_id string
, wikidata string
, datacommons_id string
, country_code string
, country_name string
, subregion1_code string
, subregion1_name string
, subregion2_code string
, subregion2_name string
, locality_code string
, locality_name string
, iso_3166_1_alpha_2 string
, iso_3166_1_alpha_3 string
, aggregation_level integer
); 

create table stg_cosmic_energy.covid_demographics
(
key string, 
population integer,
population_male integer,
population_female integer,
rural_population integer,
urban_population integer,
largest_city_population integer,
clustered_population integer,
population_density double,
human_development_index double,
population_age_00_09 integer,
population_age_10_19 integer,
population_age_20_29 integer,
population_age_30_39 integer,
population_age_40_49 integer, 
population_age_50_59 integer,
population_age_60_69 integer, 
population_age_70_79 integer,
population_age_80_and_older integer
);


create table stg_cosmic_energy.covid_hospitalizations
(
date string,
key string,
new_hospitalized_patients integer, 
new_intensive_care_patients integer,
new_ventilator_patients integer, 
cumulative_hospitalized_patients integer,
cumulative_intensive_care_patients integer,
cumulative_ventilator_patients integer,
current_hospitalized_patients integer,
current_intensive_care_patients integer,
current_ventilator_patients integer
);


create table stg_cosmic_energy.covid_vaccinations
(
date string,
key string,
new_persons_vaccinated integer,
cumulative_persons_vaccinated integer,
new_persons_fully_vaccinated integer,
cumulative_persons_fully_vaccinated integer, 
new_vaccine_doses_administered integer,
cumulative_vaccine_doses_administered integer,
new_persons_vaccinated_pfizer integer,
cumulative_persons_vaccinated_pfizer integer,
new_persons_fully_vaccinated_pfizer integer,
cumulative_persons_fully_vaccinated_pfizer integer,
new_vaccine_doses_administered_pfizer integer,
cumulative_vaccine_doses_administered_pfizer integer,
new_persons_vaccinated_moderna integer,
cumulative_persons_vaccinated_moderna integer,
new_persons_fully_vaccinated_moderna integer,
cumulative_persons_fully_vaccinated_moderna integer,
new_vaccine_doses_administered_moderna integer,
cumulative_vaccine_doses_administered_moderna integer,
new_persons_vaccinated_janssen integer,
cumulative_persons_vaccinated_janssen integer,
new_persons_fully_vaccinated_janssen integer,
cumulative_persons_fully_vaccinated_janssen integer,
new_vaccine_doses_administered_janssen integer,
cumulative_vaccine_doses_administered_janssen integer,
new_persons_vaccinated_sinovac integer,
total_persons_vaccinated_sinovac integer,
new_persons_fully_vaccinated_sinovac integer,
total_persons_fully_vaccinated_sinovac integer, 
new_vaccine_doses_administered_sinovac integer,
total_vaccine_doses_administered_sinovac integer
);