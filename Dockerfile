FROM python:3.12-slim 

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY /orchestration orchestration
COPY /radar radar
COPY ingest.py .

ENV BRONZE_GLOB=/app/data/bronze/*/*.json
ENV DAGSTER_HOME=/app/data/dagster_home
ENV CIBLE=prod

#RUN pour générer le manifest au build (dbt parse)
RUN dbt parse --project-dir "/app/radar/" --profiles-dir "/app/radar"

CMD ["sh", "-c", "mkdir -p ${DAGSTER_HOME} && exec dagster-daemon run -m orchestration.definitions"]