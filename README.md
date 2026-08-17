# Stock Data Pipeline with Airflow

A Dockerized batch data pipeline that fetches daily stock prices from Alpha Vantage, validates them, upserts raw data into PostgreSQL, and builds analytics models with dbt.

## What it demonstrates

- Apache Airflow orchestration with retries, task dependencies, and alert hooks
- Idempotent PostgreSQL ingestion using a unique `(stock_symbol, trade_date)` key
- Data validation before loading and after transformation
- dbt staging and fact models for an analytics-ready dataset
- A reproducible local environment with Docker Compose

## Architecture

```text
Alpha Vantage API
       |
       v
Airflow: availability -> fetch -> validate -> upsert -> dbt -> validation
       |                                      |
       v                                      v
PostgreSQL (raw_data)                  PostgreSQL (analytics)
       |
       +--> Slack / email notifications (optional)
```

## Prerequisites

- Docker Desktop with Docker Compose v2
- An [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key)

## Quick start

1. Create local configuration:

   ```bash
   cp .env.example .env
   ```

2. Set `ALPHA_VANTAGE_API_KEY` in `.env`. Slack and SMTP settings are optional.

3. Build and start the stack:

   ```bash
   docker compose up --build -d
   docker compose ps
   ```

4. Open Airflow at <http://localhost:8080> and sign in with `airflow` / `airflow`.

5. Unpause and trigger `stock_data_pipeline` in the Airflow UI.

To stop the stack, run `docker compose down`. Add `-v` only when you intentionally want to remove the local database volume.

## Pipeline flow

| Step | Responsibility |
| --- | --- |
| `check_api_availability` | Verifies the stock API can be reached. |
| `fetch_stock_data` | Retrieves configured symbols for the DAG execution date. |
| `validate_data_quality` | Rejects missing, invalid, or negative values. |
| `insert_raw_stock_data` | Batch-upserts records into `raw_data.raw_stock_data`. |
| `run_dbt_transformations` | Builds the staging view and analytics fact table. |
| `check_transformed_data_quality` | Checks freshness and fact-table integrity. |
| `notify_pipeline_success` | Sends an optional Slack success message. |

## Configuration

All local secrets belong in `.env`, which is ignored by Git. Start from `.env.example`; do not commit real API keys, webhooks, SMTP credentials, or production database passwords.

Useful settings include:

```env
ALPHA_VANTAGE_API_KEY=replace_me
STOCK_SYMBOLS=AAPL,GOOGL,MSFT,TSLA,AMZN
DAG_SCHEDULE_INTERVAL=@daily
SLACK_WEBHOOK_URL=
SMTP_HOST=
```

The default schedule is `@daily`. Alpha Vantage's free tier has request limits, so reduce `STOCK_SYMBOLS` if necessary.

## Project layout

```text
airflow/dags/     Airflow DAG definition
src/              ingestion, validation, and notification modules
dbt/models/       staging and mart transformations
sql/init.sql      PostgreSQL schemas and dimension setup
.github/          GitHub Actions validation workflow
```

## Verification and development

```bash
docker compose config --quiet
docker compose logs -f airflow-scheduler
docker compose exec dbt dbt debug
docker compose exec dbt dbt build --target dev
```

The GitHub Actions workflow compiles the Python modules and validates the Compose file on pushes and pull requests targeting `main`.

## Notes

This is a local, portfolio-oriented deployment. Before deploying it publicly or to production, replace default Airflow/PostgreSQL credentials, use a proper secret manager, restrict exposed ports, and choose managed infrastructure appropriate to the workload.
