"""
Stock Data Pipeline DAG

This DAG demonstrates a production-grade, idempotent data pipeline that:
1. Fetches stock market data from an external API on a schedule
2. Validates and stores raw data in PostgreSQL
3. Transforms data with dbt into analytics-ready tables
4. Handles failures gracefully with retries and alerts
5. Ensures no data duplication or corruption through idempotent design
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import logging
import os
import sys
import psycopg2
from psycopg2.extras import execute_values

# Add src to path for importing custom modules
sys.path.insert(0, '/opt/airflow/src')

from ingestion.stock_api import StockAPIClient
from alerting.slack_notifier import SlackNotifier
from alerting.email_notifier import EmailNotifier
from transforms.data_quality import DataQualityChecker

logger = logging.getLogger(__name__)

# ===== DEFAULT ARGUMENTS =====
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,  # Critical: set to False to allow independent DAG runs
    'start_date': datetime(2024, 1, 1),
    'email': [os.getenv('AIRFLOW_TO_EMAIL', 'alerts@example.com')],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': int(os.getenv('DAG_DEFAULT_RETRIES', 2)),
    'retry_delay': timedelta(minutes=int(os.getenv('DAG_RETRY_DELAY_MINUTES', 5))),
    'execution_timeout': timedelta(minutes=int(os.getenv('DAG_TIMEOUT_MINUTES', 60))),
    'pool': 'default_pool',
    'queue': 'default',
    'priority_weight': 1,
}

# ===== DAG DEFINITION =====
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Production-grade stock data ingestion and transformation pipeline',
    schedule_interval=os.getenv('DAG_SCHEDULE_INTERVAL', '@daily'),
    catchup=False,  # Don't backfill missed runs
    max_active_runs=1,  # Prevent concurrent runs of the same DAG
    tags=['data-engineering', 'production', 'stock-market'],
    doc_md=__doc__,
)

# ===== CALLBACKS =====
def on_failure_callback(context):
    """
    Send alerts on task failure.
    Triggered when a task fails and all retries are exhausted.
    """
    task_instance = context['task_instance']
    exception = context.get('exception', 'Unknown error')
    
    # Create notification message
    message = f"""
    🚨 **Airflow Task Failed**
    
    DAG: {context['dag'].dag_id}
    Task: {task_instance.task_id}
    Execution Date: {context['execution_date']}
    Exception: {exception}
    Try Number: {task_instance.try_number}
    
    Log URL: {task_instance.log_url}
    """
    
    try:
        slack_notifier = SlackNotifier()
        slack_notifier.send_alert(message, channel=os.getenv('SLACK_CHANNEL', '#airflow-alerts'))
        logger.info("Slack notification sent for failed task")
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")
    
    try:
        email_notifier = EmailNotifier()
        email_notifier.send_alert(
            subject=f"Airflow Alert: {context['dag'].dag_id} - {task_instance.task_id}",
            body=message
        )
        logger.info("Email notification sent for failed task")
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")


def on_success_callback(context):
    """
    Send notification on DAG success.
    """
    task_instance = context['task_instance']
    
    message = f"""
    ✅ **Airflow Task Succeeded**
    
    DAG: {context['dag'].dag_id}
    Task: {task_instance.task_id}
    Duration: {task_instance.duration} seconds
    """
    
    try:
        slack_notifier = SlackNotifier()
        slack_notifier.send_message(message, channel=os.getenv('SLACK_CHANNEL', '#airflow-alerts'))
    except Exception as e:
        logger.warning(f"Failed to send success notification: {e}")


# Attach callbacks to DAG
dag.on_failure_callback = on_failure_callback

# ===== TASKS =====

# Task 1: Check data source availability
def check_api_availability():
    """
    Pre-flight check: Verify external API is accessible.
    This prevents unnecessary work if the data source is unavailable.
    """
    logger.info("Checking API availability...")
    api_client = StockAPIClient()
    
    try:
        api_client.health_check()
        logger.info("API is accessible")
        return {'status': 'healthy', 'timestamp': datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"API health check failed: {e}")
        raise AirflowException(f"API unavailable: {e}")


check_api_task = PythonOperator(
    task_id='check_api_availability',
    python_callable=check_api_availability,
    dag=dag,
    on_failure_callback=on_failure_callback,
)

# Task 2: Fetch stock data from API
def fetch_stock_data(**context):
    """
    Fetch stock market data from Alpha Vantage/Finnhub API.
    
    Idempotency principle: This task is idempotent because:
    - It fetches data for a specific date (from execution_date)
    - Data is stored using upsert logic, so re-running doesn't duplicate
    - The raw_stock_data table has a unique constraint on (stock_symbol, date)
    """
    execution_date = context['execution_date']
    logger.info(f"Fetching stock data for {execution_date.date()}")
    
    api_client = StockAPIClient()
    symbols = os.getenv('STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT,TSLA,AMZN').split(',')
    
    all_data = []
    for symbol in symbols:
        try:
            data = api_client.get_daily_data(
                symbol=symbol,
                date=execution_date.date()
            )
            if data:
                all_data.append(data)
                logger.info(f"Fetched {len(data)} records for {symbol}")
        except Exception as e:
            logger.warning(f"Failed to fetch data for {symbol}: {e}")
    
    if not all_data:
        raise AirflowException("No data fetched from API")
    
    # Push data to XCom for downstream tasks
    context['task_instance'].xcom_push(key='raw_stock_data', value=all_data)
    logger.info(f"Fetched total {sum(len(d) for d in all_data)} records")
    
    return {'records_fetched': sum(len(d) for d in all_data)}


fetch_data_task = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_stock_data,
    provide_context=True,
    dag=dag,
    on_failure_callback=on_failure_callback,
)

# Task 3: Validate data quality
def validate_data_quality(**context):
    """
    Perform data quality checks on fetched data.
    """
    logger.info("Starting data quality validation...")
    
    raw_data = context['task_instance'].xcom_pull(
        task_ids='fetch_stock_data',
        key='raw_stock_data'
    )
    
    records = [record for symbol_records in raw_data for record in symbol_records]
    quality_checker = DataQualityChecker()
    validation_results = quality_checker.validate_raw_data(records)
    
    if not validation_results['is_valid']:
        raise AirflowException(f"Data quality checks failed: {validation_results['errors']}")
    
    logger.info(f"Data validation passed: {validation_results['summary']}")
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    
    return validation_results


validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag,
    on_failure_callback=on_failure_callback,
)

# Task 4: Insert API records into PostgreSQL (idempotent with ON CONFLICT)
def load_raw_stock_data(**context):
    records_by_symbol = context['task_instance'].xcom_pull(
        task_ids='fetch_stock_data', key='raw_stock_data'
    )
    records = [record for symbol_records in records_by_symbol for record in symbol_records]
    if not records:
        raise AirflowException('No records available to load')

    values = [
        (
            record['stock_symbol'], record['date'], record['open'], record['close'],
            record['high'], record['low'], record['volume'], record.get('source', 'alpha_vantage'),
        )
        for record in records
    ]
    connection = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'airflow'),
        password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
        dbname=os.getenv('POSTGRES_DB', 'airflow'),
    )
    try:
        with connection, connection.cursor() as cursor:
            execute_values(
                cursor,
                '''
                INSERT INTO raw_data.raw_stock_data
                    (stock_symbol, trade_date, open_price, close_price, high_price, low_price, volume, source)
                VALUES %s
                ON CONFLICT (stock_symbol, trade_date) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    close_price = EXCLUDED.close_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    volume = EXCLUDED.volume,
                    source = EXCLUDED.source,
                    updated_at = CURRENT_TIMESTAMP
                ''',
                values,
            )
    finally:
        connection.close()
    return {'records_loaded': len(values)}


insert_raw_data_task = PythonOperator(
    task_id='insert_raw_stock_data',
    python_callable=load_raw_stock_data,
    provide_context=True,
    dag=dag,
    on_failure_callback=on_failure_callback,
)

# Task 5: Run dbt for transformations
run_dbt_task = BashOperator(
    task_id='run_dbt_transformations',
    bash_command="""
    cd /dbt && \
    dbt run --target prod --profiles-dir /dbt --project-dir /dbt && \
    dbt test --target prod --profiles-dir /dbt --project-dir /dbt
    """,
    dag=dag,
    on_failure_callback=on_failure_callback,
    retries=1,  # Lower retry count for dbt tasks
)

# Task 6: Data quality checks on transformed data
def check_transformed_data_quality():
    """
    Verify transformed data meets business requirements.
    """
    logger.info("Checking transformed data quality...")
    # This would connect to the analytics schema and validate
    quality_checker = DataQualityChecker()
    results = quality_checker.validate_transformed_data()
    
    if not results['is_valid']:
        raise AirflowException(f"Transformed data quality checks failed: {results['errors']}")
    
    logger.info(f"Transformed data quality check passed")
    return results


check_transformed_task = PythonOperator(
    task_id='check_transformed_data_quality',
    python_callable=check_transformed_data_quality,
    dag=dag,
    on_failure_callback=on_failure_callback,
)

# Task 7: Notify success
def notify_pipeline_success(**context):
    """
    Send success notification with pipeline statistics.
    """
    logger.info("Pipeline completed successfully!")
    
    message = f"""
    ✅ **Stock Data Pipeline Completed Successfully**
    
    Execution Date: {context['execution_date'].date()}
    DAG: {context['dag'].dag_id}
    Duration: {context['task_instance'].duration} seconds
    
    Next scheduled run: Tomorrow at the same time
    """
    
    try:
        slack_notifier = SlackNotifier()
        slack_notifier.send_message(message, channel=os.getenv('SLACK_CHANNEL', '#airflow-alerts'))
    except Exception as e:
        logger.warning(f"Failed to send success notification: {e}")


notify_success_task = PythonOperator(
    task_id='notify_pipeline_success',
    python_callable=notify_pipeline_success,
    provide_context=True,
    dag=dag,
    trigger_rule='all_success',
)

# ===== TASK DEPENDENCIES =====
"""
DAG Structure:

check_api_availability
    └── fetch_stock_data
            └── validate_data_quality
                    └── insert_raw_stock_data
                            └── run_dbt_transformations
                                    ├── check_transformed_data_quality
                                    └── notify_pipeline_success
"""

check_api_task >> fetch_data_task
fetch_data_task >> validate_task
validate_task >> insert_raw_data_task
insert_raw_data_task >> run_dbt_task
run_dbt_task >> check_transformed_task
check_transformed_task >> notify_success_task
