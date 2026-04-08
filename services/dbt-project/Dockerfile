FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    dbt-core \
    dbt-postgres \
    openlineage-dbt>=1.39.0

COPY . .

# dbt needs profiles in ~/.dbt/ or via --profiles-dir
RUN mkdir -p /root/.dbt && cp profiles.yml /root/.dbt/profiles.yml

CMD ["tail", "-f", "/dev/null"]
