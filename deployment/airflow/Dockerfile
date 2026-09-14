FROM apache/airflow:2.10.2-python3.12

COPY requirements.txt /tmp/gates-requirements.txt

# Installed in separate layers rather than one `pip install -r
# requirements.txt`: combining dbt-core, great-expectations, and
# acryl-datahub in a single resolve makes pip's dependency resolver
# backtrack through many versions of shared transitive deps (altair,
# dbt_common, click, jinja2, etc.), sometimes taking 20-40+ minutes or
# failing to resolve at all. Installing incrementally means each step only
# needs to stay compatible with what's already installed — a much smaller,
# faster search — and any real incompatibility fails fast with a clear
# error naming the two packages in conflict, instead of a silent multi-hour
# hang.
RUN pip install --no-cache-dir \
    "pandas>=2.2,<3" "pyyaml>=6,<7" "requests>=2.32,<3" "openpyxl>=3.1,<4" \
    "confluent-kafka>=2.5,<3" "sqlalchemy>=2.0,<3" "psycopg2-binary>=2.9,<3" \
    "pyarrow>=17,<19" "trino>=0.328,<1"
RUN pip install --no-cache-dir "great-expectations>=1.2,<2"
RUN pip install --no-cache-dir "dbt-core>=1.8,<2" "dbt-trino>=1.8,<2"
RUN pip install --no-cache-dir "acryl-datahub>=0.14,<1"
RUN pip install --no-cache-dir "airbyte-cdk>=0.90,<2"
