# Quick Start Guide

Get the pipeline running in 5 minutes.

## Step 1: Get API Keys

### Alpha Vantage API Key
- Go to: https://www.alphavantage.co/
- Sign up for free
- Copy your API key

### Slack Webhook URL (Optional)
- Go to: https://api.slack.com/messaging/webhooks
- Create a new app
- Copy the Webhook URL

## Step 2: Configure Environment

```bash
# Navigate to project directory
cd "Airflow Batch Orchestration Project"

# Edit .env file
# Replace YOUR_KEYS with actual values:
ALPHA_VANTAGE_API_KEY=YOUR_API_KEY_HERE
SLACK_WEBHOOK_URL=YOUR_WEBHOOK_HERE
SLACK_TOKEN=YOUR_BOT_TOKEN_HERE
```

## Step 3: Start Services

```bash
# Start all containers
docker-compose up -d

# Verify containers are running
docker-compose ps

# Wait for Airflow to be ready (check logs)
docker-compose logs airflow-webserver

# Look for: "Loaded 1 DAG"
```

## Step 4: Access Airflow

1. Open browser: http://localhost:8080
2. Login with:
   - Username: `airflow`
   - Password: `airflow`

3. You should see `stock_data_pipeline` DAG

## Step 5: Trigger DAG

1. Click on `stock_data_pipeline`
2. Click the play button (▶) in top-right
3. Select "Trigger DAG" → "Trigger"
4. Wait for execution

## Step 6: Monitor Execution

- **Watch Task Progress**: Look at the graph view
- **Check Logs**: Click task → "Logs"
- **Verify Data**: Connect to PostgreSQL:

```bash
# Access PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# Check raw data loaded
SELECT COUNT(*), stock_symbol FROM raw_data.raw_stock_data 
GROUP BY stock_symbol;

# Check transformed data
SELECT COUNT(*) FROM analytics.fct_stock_price;
```

## Troubleshooting

### Services won't start
```bash
# Check logs
docker-compose logs

# Restart everything
docker-compose down
docker-compose up -d --build
```

### API Key Error
```bash
# Test API key works
curl "https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=AAPL&apikey=YOUR_KEY"

# Should return JSON with stock data (not error)
```

### Airflow UI won't load
```bash
# Wait 30 seconds, then reload page
# If still failing, check logs:
docker-compose logs airflow-webserver
```

### PostgreSQL connection error
```bash
# Verify PostgreSQL is healthy
docker-compose exec postgres pg_isready

# Should return: "accepting connections"
```

## Next Steps

### Modify Schedule
Edit `airflow/dags/stock_data_pipeline.py`:
```python
schedule_interval='@daily'  # Change to '@hourly' or specific time
```

### Add More Stocks
Edit `airflow/dags/stock_data_pipeline.py`:
```python
symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'NVDA']  # Add NVDA
```

### Add Email Alerts
Configure in `.env`:
```env
SMTP_HOST=smtp.gmail.com
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-specific-password
AIRFLOW_TO_EMAIL=alerts@company.com
```

## Common Commands

```bash
# View all logs
docker-compose logs

# View specific service logs
docker-compose logs airflow-scheduler -f

# Stop all services
docker-compose down

# Stop and delete data
docker-compose down -v

# Restart everything
docker-compose restart

# Execute command in container
docker-compose exec airflow-scheduler airflow dags list

# View database
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM raw_data.raw_stock_data LIMIT 5;"
```

## Understanding the Pipeline

### What Happens When DAG Runs

1. **Check API** ✓
   - Verifies Alpha Vantage is accessible

2. **Fetch Data** ✓
   - Pulls daily prices for AAPL, GOOGL, MSFT, TSLA, AMZN

3. **Validate Quality** ✓
   - Checks for nulls, bad values, duplicates

4. **Load to Raw** ✓
   - Stores in PostgreSQL (idempotent, no duplicates)

5. **Transform with dbt** ✓
   - Cleans, enriches, and joins with dimensions

6. **Check Results** ✓
   - Final validation on transformed data

7. **Alert** ✓
   - Sends success notification to Slack

### Data Flow

```
API (Alpha Vantage)
    ↓
Raw Table (PostgreSQL)
    ↓
Staging View (dbt)
    ↓
Analytics Table (dbt)
    ↓
Dashboards / Reports / ML Models
```

## Where to Find Things

| What | Where |
|------|-------|
| DAG Logic | `airflow/dags/stock_data_pipeline.py` |
| API Client | `src/ingestion/stock_api.py` |
| Data Validation | `src/transforms/data_quality.py` |
| Slack Alerts | `src/alerting/slack_notifier.py` |
| Email Alerts | `src/alerting/email_notifier.py` |
| Database Schema | `sql/init.sql` |
| Data Transforms | `dbt/models/` |
| Configuration | `.env` |

## Support

Stuck? Check:

1. **README.md** - Full documentation
2. **Docker logs**: `docker-compose logs`
3. **Airflow logs**: Airflow UI → Task → Logs
4. **PostgreSQL**: `docker-compose exec postgres psql`

---
