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
# Each RUN below installs a filtered subset of lines straight out of
# requirements.txt (grep -E, one pattern per layer-grouping) so the file
# stays the single source of truth — editing a version constraint there is
# enough, no separate copy to keep in sync in this Dockerfile.
# dbt-core/dbt-trino and airbyte-cdk both constrain protobuf (dbt needs
# >=6.0, airbyte-cdk's google-cloud-secret-manager dependency needs <6.0)
# — genuinely incompatible, not just a slow resolve. dbt is central to this
# pipeline and airbyte-cdk's GCP Secret Manager code path is never actually
# exercised here, so dbt-core/dbt-trino installs LAST: whichever protobuf
# version its own install step needs wins the final environment.
RUN pip install --no-cache-dir $(grep -E '^(pandas|pyyaml|requests|openpyxl|confluent-kafka|sqlalchemy|psycopg2-binary|pyarrow|trino)[>=<]' /tmp/gates-requirements.txt)
RUN pip install --no-cache-dir $(grep -E '^great-expectations[>=<]' /tmp/gates-requirements.txt)
# Not "^acryl-datahub[>=<]" — that character class stops matching the
# moment the line gains an extras marker like "[dbt]" right after the
# package name, silently installing nothing instead of failing loudly.
RUN pip install --no-cache-dir $(grep -E '^acryl-datahub(\[|[>=<])' /tmp/gates-requirements.txt)
RUN pip install --no-cache-dir $(grep -E '^airbyte-cdk[>=<]' /tmp/gates-requirements.txt)
RUN pip install --no-cache-dir $(grep -E '^(dbt-core|dbt-trino)[>=<]' /tmp/gates-requirements.txt)
# Installed LAST, after every other stage, so it wins regardless of which
# earlier stage's resolve pulled in an older pyOpenSSL alongside boto3 —
# see requirements.txt's comment for why this pin exists at all.
RUN pip install --no-cache-dir $(grep -E '^pyopenssl[>=<]' /tmp/gates-requirements.txt)
