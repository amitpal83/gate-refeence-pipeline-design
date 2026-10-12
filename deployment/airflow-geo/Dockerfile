# Geo + PII environment, layered on top of the existing Airflow image.
#
# Build order: gates-demo-airflow:local must exist first (docker-compose.demo.yml
# builds it). deployment/airflow/Dockerfile and requirements.txt are not edited.
#
# The packages go in their own virtualenv instead of Airflow's environment.
# geopandas, spaCy and Presidio pull in their own pandas, numpy and click
# versions, and installing them next to dbt and Great Expectations could break
# the existing pipelines. Geo tasks run in this environment through
# ExternalPythonOperator (planned step S11), using GEO_PYTHON.
FROM gates-demo-airflow:local

USER airflow

COPY requirements-geo.txt /tmp/requirements-geo.txt

RUN python -m venv /opt/airflow/geo-venv \
 && /opt/airflow/geo-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/airflow/geo-venv/bin/pip install --no-cache-dir -r /tmp/requirements-geo.txt

# Separate layer: the model is large, so a change to requirements above does not
# re-download it unless the first layer changed. `spacy download` selects the
# model release that matches the installed spaCy.
RUN /opt/airflow/geo-venv/bin/python -m spacy download en_core_web_lg
