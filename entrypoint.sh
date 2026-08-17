#!/bin/bash
set -e

# Wait for database to be ready
echo "Waiting for database to be ready..."
while ! pg_isready -h postgres -U airflow > /dev/null 2>&1; do
    echo "Database is unavailable - sleeping"
    sleep 2
done
echo "Database is ready"

# Initialize Airflow database if needed
echo "Initializing Airflow database..."
airflow db init

# Create default user if needed
airflow users create \
    --username airflow \
    --password airflow \
    --firstname Air \
    --lastname Flow \
    --role Admin \
    --email airflow@example.com 2>/dev/null || true

# Execute the passed command
exec "$@"
