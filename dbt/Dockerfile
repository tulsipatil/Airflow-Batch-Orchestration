FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install dbt
RUN pip install --no-cache-dir \
    dbt-core==1.7.4 \
    dbt-postgres==1.7.4 \
    python-dotenv==1.0.0

WORKDIR /dbt

ENV DBT_PROFILES_DIR=/dbt

CMD ["dbt", "version"]
