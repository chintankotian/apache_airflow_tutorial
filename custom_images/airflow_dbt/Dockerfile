FROM  apache/airflow:latest

RUN python3 -m pip install dbt-core dbt-postgres

USER root
RUN mkdir -p /root/.dbt
COPY /.dbt/profiles.yml /root/.dbt/profiles.yml

WORKDIR /app

RUN cd /app 



# RUN dbt init weather_etl --profile weather_etl

# RUN ls -r .