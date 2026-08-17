-- dbt generic tests for stock analytics models

-- Test: No null values in fact table keys
-- dbt test --select tag:critical

-- Test: All stock_ids exist in dimension
-- dbt test --select tag:referential_integrity

-- Test: Fact table has recent data
-- Trigger: on-run-end hooks

-- Test: Price relationships are valid
-- dbt test --select model_name --severity error

-- Example dbt test to check for duplicate fact rows
-- File: dbt/tests/assert_no_duplicates_fact.sql

SELECT
    stock_id,
    date_id,
    COUNT(*) as dup_count
FROM {{ ref('fct_stock_price') }}
GROUP BY stock_id, date_id
HAVING COUNT(*) > 1

-- If this returns any rows, the test fails
