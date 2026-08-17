{{
  config(
    materialized='table',
    tags=['mart', 'fact'],
    description='Fact table for stock prices joined with dimensions. Uses star schema for analytics.',
    indexes=[
      {'columns': ['stock_id']},
      {'columns': ['date_id']},
      {'columns': ['stock_id', 'date_id'], 'unique': true}
    ]
  )
}}

WITH stg_stock AS (
    SELECT * FROM {{ ref('stg_stock_data') }}
),

dim_stock AS (
    SELECT 
        stock_id,
        stock_symbol
    FROM {{ source('analytics', 'dim_stock') }}
),

dim_date AS (
    SELECT 
        date_id,
        calendar_date
    FROM {{ source('analytics', 'dim_date') }}
)

, ranked_stock_prices AS (
    SELECT
        stg_stock.*,
        ds.stock_id,
        dd.date_id,
        ROW_NUMBER() OVER (
            PARTITION BY ds.stock_id, dd.date_id
            ORDER BY stg_stock.processed_at DESC
        ) AS row_num
    FROM stg_stock
    INNER JOIN dim_stock ds
        ON stg_stock.stock_symbol = ds.stock_symbol
    INNER JOIN dim_date dd
        ON stg_stock.trade_date = dd.calendar_date
    WHERE stg_stock.data_quality_flag = 'VALID'
)

SELECT
    md5(concat_ws('||', stock_id::text, date_id::text)) AS stock_price_id,
    stock_id,
    date_id,
    open_price,
    close_price,
    high_price,
    low_price,
    volume,
    daily_change AS price_change,
    percent_change,
    high_low_range AS days_high_low,
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at

FROM ranked_stock_prices
WHERE row_num = 1
