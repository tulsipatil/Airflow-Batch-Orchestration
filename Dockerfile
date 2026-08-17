FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --user --no-cache-dir -r /requirements.txt

# Make the dbt project available to the Airflow tasks that invoke dbt.
COPY --chown=airflow:root dbt /dbt

WORKDIR /opt/airflow

ENTRYPOINT ["/entrypoint.sh"]
