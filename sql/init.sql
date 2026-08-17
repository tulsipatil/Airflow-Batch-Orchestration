-- ===== INITIALIZATION SCRIPT FOR AIRFLOW POSTGRES DATABASE =====
-- This script creates the necessary schemas, tables, and functions for the stock data pipeline

-- ===== SCHEMAS =====
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ===== RAW DATA SCHEMA =====

-- Raw stock data table (landing zone for API data)
CREATE TABLE IF NOT EXISTS raw_data.raw_stock_data (
    raw_stock_data_id BIGSERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,
    open_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    volume BIGINT,
    source VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint to ensure idempotent inserts
    UNIQUE(stock_symbol, trade_date)
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_raw_stock_data_symbol ON raw_data.raw_stock_data(stock_symbol);
CREATE INDEX IF NOT EXISTS idx_raw_stock_data_date ON raw_data.raw_stock_data(trade_date);
CREATE INDEX IF NOT EXISTS idx_raw_stock_data_loaded ON raw_data.raw_stock_data(loaded_at);

-- Data quality issues tracking
CREATE TABLE IF NOT EXISTS raw_data.data_quality_log (
    dq_log_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    issue_type VARCHAR(50),
    issue_description TEXT,
    affected_records BIGINT,
    severity VARCHAR(20), -- LOW, MEDIUM, HIGH
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    resolution_notes TEXT
);

-- ===== ANALYTICS SCHEMA =====

-- Dimension: Stock symbols
CREATE TABLE IF NOT EXISTS analytics.dim_stock (
    stock_id SERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_stock_symbol ON analytics.dim_stock(stock_symbol);

-- Dimension: Date (slowly changing dimension)
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_id SERIAL PRIMARY KEY,
    calendar_date DATE UNIQUE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_of_week INT,
    week_of_year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    business_day BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_date_calendar ON analytics.dim_date(calendar_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON analytics.dim_date(year, month);

-- Fact table: managed by the dbt model `fct_stock_price`.

-- Aggregate: Daily price statistics by stock
CREATE TABLE IF NOT EXISTS analytics.agg_daily_stock_stats (
    agg_id BIGSERIAL PRIMARY KEY,
    stock_id INT NOT NULL,
    date_id INT NOT NULL,
    avg_close_price NUMERIC(10, 2),
    max_high_price NUMERIC(10, 2),
    min_low_price NUMERIC(10, 2),
    total_volume BIGINT,
    num_trades INT,
    volatility NUMERIC(10, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_agg_stock_id FOREIGN KEY (stock_id) REFERENCES analytics.dim_stock(stock_id),
    CONSTRAINT fk_agg_date_id FOREIGN KEY (date_id) REFERENCES analytics.dim_date(date_id),
    UNIQUE(stock_id, date_id)
);

-- ===== MONITORING SCHEMA =====

-- Pipeline execution log
CREATE TABLE IF NOT EXISTS monitoring.pipeline_execution_log (
    execution_id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    execution_date TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50), -- running, success, failed
    error_message TEXT,
    records_processed BIGINT,
    duration_seconds NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_exec_dag ON monitoring.pipeline_execution_log(dag_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_exec_status ON monitoring.pipeline_execution_log(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_exec_date ON monitoring.pipeline_execution_log(execution_date);

-- SLA monitoring
CREATE TABLE IF NOT EXISTS monitoring.sla_violations (
    violation_id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    expected_duration_seconds INT,
    actual_duration_seconds NUMERIC(10, 2),
    execution_date TIMESTAMP,
    violation_type VARCHAR(50), -- timeout, data_quality, resource
    severity VARCHAR(20),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMP
);

-- ===== HELPER FUNCTIONS & PROCEDURES =====

-- Function to populate dim_date table
CREATE OR REPLACE FUNCTION analytics.populate_dim_date(start_date DATE, end_date DATE) 
RETURNS void AS $$
DECLARE
    loop_date DATE;
BEGIN
    loop_date := start_date;
    
    WHILE loop_date <= end_date LOOP
        INSERT INTO analytics.dim_date (
            calendar_date, year, quarter, month, day, day_of_week, 
            week_of_year, is_weekend, is_holiday, business_day
        ) VALUES (
            loop_date,
            EXTRACT(YEAR FROM loop_date)::INT,
            EXTRACT(QUARTER FROM loop_date)::INT,
            EXTRACT(MONTH FROM loop_date)::INT,
            EXTRACT(DAY FROM loop_date)::INT,
            EXTRACT(DOW FROM loop_date)::INT,
            EXTRACT(WEEK FROM loop_date)::INT,
            EXTRACT(DOW FROM loop_date) IN (0, 6), -- Saturday and Sunday
            FALSE, -- Set to TRUE for holidays manually
            EXTRACT(DOW FROM loop_date) NOT IN (0, 6)
        ) ON CONFLICT (calendar_date) DO NOTHING;
        
        loop_date := loop_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate date dimension (past 2 years and future 1 year)
SELECT analytics.populate_dim_date(
    (CURRENT_DATE - INTERVAL '2 years')::DATE, 
    (CURRENT_DATE + INTERVAL '1 year')::DATE
);

-- Function to insert stock symbols
CREATE OR REPLACE FUNCTION analytics.upsert_stock_symbol(p_symbol VARCHAR(10), p_company_name VARCHAR(255)) 
RETURNS void AS $$
BEGIN
    INSERT INTO analytics.dim_stock (stock_symbol, company_name)
    VALUES (p_symbol, p_company_name)
    ON CONFLICT (stock_symbol) DO UPDATE 
    SET company_name = EXCLUDED.company_name, updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- Seed initial stock symbols
SELECT analytics.upsert_stock_symbol('AAPL', 'Apple Inc.');
SELECT analytics.upsert_stock_symbol('GOOGL', 'Alphabet Inc.');
SELECT analytics.upsert_stock_symbol('MSFT', 'Microsoft Corporation');
SELECT analytics.upsert_stock_symbol('TSLA', 'Tesla Inc.');
SELECT analytics.upsert_stock_symbol('AMZN', 'Amazon.com Inc.');

-- Function to check data quality and log issues
CREATE OR REPLACE FUNCTION raw_data.log_data_quality_issue(
    p_table_name VARCHAR(100),
    p_issue_type VARCHAR(50),
    p_issue_description TEXT,
    p_affected_records BIGINT,
    p_severity VARCHAR(20)
) RETURNS void AS $$
BEGIN
    INSERT INTO raw_data.data_quality_log (table_name, issue_type, issue_description, affected_records, severity)
    VALUES (p_table_name, p_issue_type, p_issue_description, p_affected_records, p_severity);
END;
$$ LANGUAGE plpgsql;

-- Grant permissions (adjust as needed for your users)
GRANT USAGE ON SCHEMA raw_data TO airflow;
GRANT USAGE ON SCHEMA analytics TO airflow;
GRANT USAGE ON SCHEMA monitoring TO airflow;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA monitoring TO airflow;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw_data TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA analytics TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA monitoring TO airflow;

-- Enable query logging for debugging (optional)
-- ALTER SYSTEM SET log_statement = 'all';
-- SELECT pg_reload_conf();

-- ===== FINAL NOTES =====
-- Tables created: raw_stock_data, dim_stock, dim_date, fct_stock_price, etc.
-- Schemas created: raw_data, analytics, monitoring
-- This setup ensures idempotent data loads through UNIQUE constraints and ON CONFLICT clauses
-- All timestamps use CURRENT_TIMESTAMP for consistency
