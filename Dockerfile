# syntax=docker/dockerfile:1.4

FROM python:3.8-slim as py38
COPY snowpark_requirements.txt snowpark_requirements.txt 
RUN pip install --no-cache-dir -r snowpark_requirements.txt


FROM quay.io/astronomer/astro-runtime:7.0.0

##### Docker Customizations below this line #####

## If you are using OpenLineage Custom Extactors
ENV OPENLINEAGE_EXTRACTORS=include.extractors.s3_snowflake_extractor.S3ToSnowflakeOperatorExtractor;include.extractors.gcs_to_s3_extractor.GCSToS3OperatorExtractor;include.extractors.s3_to_azure_extractor.S3ToAzureBlobOperatorExtractor

### Secrets from AWS Parameter Store
ENV AIRFLOW__SECRETS__BACKEND="airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend"
ENV AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_prefix": "/airflow/connections", "variables_prefix": "/airflow/variables"}'

## Required for astro-sdk
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
# ENV AIRFLOW__ASTRO_SDK__SQL_SCHEMA='aws_stg_cosmic_energy'

### Email Setup
ENV AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.providers.sendgrid.utils.emailer.send_email
ENV AIRFLOW__EMAIL__FROM_EMAIL=manmeetkaur.rangoola@gmail.com

### for seeing the airflow configurations via Airflow UI
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

### Enabling Debug Logging
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG

### Put it in the Astro Cloud UI if you are putting the secret key here 
# ENV AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://?extra__google_cloud_platform__key_path=%2Fusr%2Flocal%2Fairflow%2Finclude%2Fkey.json&extra__google_cloud_platform__project=astronomer-field&extra__google_cloud_platform__num_retries=2'

#### Only for Local #####
# set these 3 lines for local lineage
# ENV AIRFLOW__LINEAGE__BACKEND=openlineage.lineage_backend.OpenLineageBackend
# ENV OPENLINEAGE_URL=http://host.docker.internal:3000
# ENV OPENLINEAGE_NAMESPACE="default"

### if you want to disable Lineage set these 2 lines
# ENV AIRFLOW__LINEAGE__BACKEND=
# ENV OPENLINEAGE_DISABLED=True


# # Settings for using ExternalPythonOperator 
# ENV PYENV_ROOT="/home/astro/.pyenv"
# ENV PATH=${PYENV_ROOT}/bin:${PATH}

# # RUN pip-compile -h
# # RUN pip-compile snowpark_requirements.6.txt

# RUN curl https://pyenv.run | bash  && \
#     eval "$(pyenv init -)" && \
#     pyenv install 3.8.14 && \
#     pyenv virtualenv 3.8.14 snowpark_env && \
#     pyenv activate snowpark_env && \
#     pip install --no-cache-dir --upgrade pip && \
#     pip install --no-cache-dir -r snowpark_requirements.txt

COPY --from=py38 /usr/local/bin/*3.8* /usr/local/bin/
COPY --from=py38 /usr/local/lib/pkgconfig/*3.8* /usr/local/pkgconfig/
COPY --from=py38 /usr/local/lib/*3.8*.so  /usr/local/lib/
COPY --from=py38 /usr/local/lib/python3.8  /usr/local/lib/python3.8

USER root
RUN ldconfig
USER astro