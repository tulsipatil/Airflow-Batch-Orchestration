{{
  config(
    materialized='view',
    schema='staging',
    tags=['staging'],
    description='Staging view of raw stock data with basic transformations and data quality checks'
  )
}}

SELECT
    raw_stock_data_id,
    stock_symbol,
    trade_date,
    COALESCE(open_price, 0) as open_price,
    COALESCE(close_price, 0) as close_price,
    COALESCE(high_price, 0) as high_price,
    COALESCE(low_price, 0) as low_price,
    COALESCE(volume, 0) as volume,
    source,
    loaded_at,
    CURRENT_TIMESTAMP as processed_at,
    
    -- Calculated fields
    ROUND(close_price - open_price, 2) as daily_change,
    CASE 
        WHEN open_price > 0 THEN ROUND((close_price - open_price) / open_price * 100, 4)
        ELSE 0
    END as percent_change,
    ROUND(high_price - low_price, 2) as high_low_range,
    
    -- Data quality flags
    CASE 
        WHEN low_price > high_price THEN 'HIGH_LOW_INVALID'
        WHEN close_price > high_price OR close_price < low_price THEN 'CLOSE_OUT_OF_RANGE'
        WHEN volume < 0 THEN 'NEGATIVE_VOLUME'
        WHEN open_price < 0 OR close_price < 0 THEN 'NEGATIVE_PRICE'
        ELSE 'VALID'
    END as data_quality_flag
    
FROM {{ source('raw_data', 'raw_stock_data') }}

WHERE 
    -- Exclude data quality issues for production view
    CASE 
        WHEN low_price > high_price THEN FALSE
        WHEN volume < 0 THEN FALSE
        ELSE TRUE
    END
    
ORDER BY trade_date DESC, stock_symbol
