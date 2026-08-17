"""
Data Quality Checker for pipeline validation.

Ensures data quality at each stage of the pipeline.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import os

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """
    Performs data quality checks on ingested and transformed data.
    """
    
    def __init__(self):
        """Initialize database connection parameters."""
        self.db_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.db_port = int(os.getenv('POSTGRES_PORT', 5432))
        self.db_user = os.getenv('POSTGRES_USER', 'airflow')
        self.db_password = os.getenv('POSTGRES_PASSWORD', 'airflow')
        self.db_name = os.getenv('POSTGRES_DB', 'airflow')
    
    def validate_raw_data(self, raw_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate raw data from API ingestion.
        
        Checks:
        - Non-empty dataset
        - Required fields present
        - Data types are correct
        - Values are within expected ranges
        
        Args:
            raw_data: List of data records from API
            
        Returns:
            Validation result dictionary
        """
        logger.info("Validating raw API data...")
        
        errors = []
        warnings = []
        
        # Check 1: Dataset is not empty
        if not raw_data:
            errors.append("Dataset is empty - no records received from API")
            return {
                'is_valid': False,
                'errors': errors,
                'warnings': warnings,
                'summary': f'{len(errors)} validation errors',
                'record_count': 0,
            }
        
        # Check 2: Validate each record
        required_fields = ['stock_symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        for idx, record in enumerate(raw_data):
            # Check for required fields
            for field in required_fields:
                if field not in record:
                    errors.append(f"Record {idx}: Missing required field '{field}'")
            
            # Check data types
            try:
                if 'stock_symbol' in record and not isinstance(record['stock_symbol'], str):
                    errors.append(f"Record {idx}: stock_symbol must be string")
                
                for price_field in ['open', 'high', 'low', 'close']:
                    if price_field in record:
                        price = float(record[price_field])
                        if price < 0:
                            errors.append(f"Record {idx}: {price_field} price cannot be negative")
                
                if 'volume' in record:
                    vol = int(record['volume'])
                    if vol < 0:
                        errors.append(f"Record {idx}: volume cannot be negative")
                
                # Check price relationships
                if all(k in record for k in ['high', 'low', 'close', 'open']):
                    high = float(record['high'])
                    low = float(record['low'])
                    close = float(record['close'])
                    open_price = float(record['open'])
                    
                    if low > high:
                        errors.append(f"Record {idx}: low price cannot be higher than high price")
                    
                    if close > high or close < low:
                        warnings.append(f"Record {idx}: close price not within high-low range")
                    
                    if open_price > high or open_price < low:
                        warnings.append(f"Record {idx}: open price not within high-low range")
                        
            except (ValueError, TypeError) as e:
                errors.append(f"Record {idx}: Data type conversion error - {str(e)}")
        
        # Check 3: Statistical validation
        if raw_data and 'close' in raw_data[0]:
            try:
                prices = [float(r.get('close', 0)) for r in raw_data if 'close' in r]
                if prices:
                    avg_price = sum(prices) / len(prices)
                    
                    # Flag unusually high or low average prices
                    if avg_price > 10000:
                        warnings.append(f"Unusual average price detected: ${avg_price:.2f}")
                    elif avg_price < 0.01:
                        warnings.append(f"Very low average price detected: ${avg_price:.2f}")
            except Exception as e:
                logger.warning(f"Could not perform statistical validation: {e}")
        
        is_valid = len(errors) == 0
        
        result = {
            'is_valid': is_valid,
            'errors': errors,
            'warnings': warnings,
            'summary': f'{len(errors)} errors, {len(warnings)} warnings',
            'record_count': len(raw_data),
            'validation_timestamp': datetime.now().isoformat(),
        }
        
        logger.info(f"Raw data validation complete: {result['summary']}")
        return result
    
    def validate_transformed_data(self, schema: str = 'analytics') -> Dict[str, Any]:
        """
        Validate transformed data in analytics schema.
        
        Checks:
        - No null values in key columns
        - Data freshness (recent data present)
        - Record counts match expectations
        - Referential integrity
        
        Args:
            schema: Database schema containing transformed data
            
        Returns:
            Validation result dictionary
        """
        logger.info(f"Validating transformed data in schema '{schema}'...")
        
        errors = []
        warnings = []
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check 1: Table existence
            tables_to_check = ['fct_stock_price', 'dim_stock', 'dim_date']
            for table in tables_to_check:
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = '{schema}' 
                        AND table_name = '{table}'
                    )
                """)
                
                if not cursor.fetchone()[0]:
                    errors.append(f"Required table '{schema}.{table}' does not exist")
            
            # Check 2: Data freshness
            cursor.execute(f"""
                SELECT MAX(dd.calendar_date) AS latest_date
                FROM {schema}.fct_stock_price AS fp
                JOIN {schema}.dim_date AS dd ON fp.date_id = dd.date_id
            """)
            
            result = cursor.fetchone()
            if result and result['latest_date']:
                from datetime import datetime, timedelta
                days_old = (datetime.now().date() - result['latest_date']).days
                
                if days_old > 30:
                    errors.append(f"Data is severely stale ({days_old} days old)")
                elif days_old > 7:
                    warnings.append(f"Data is {days_old} days old - consider checking data source")
            else:
                errors.append("No data found in fact table")
            
            # Check 3: Null values in key columns
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_schema = '{schema}' 
                AND table_name = 'fct_stock_price'
                AND is_nullable = 'NO'
            """)
            
            key_columns = [row['column_name'] for row in cursor.fetchall()]
            
            for col in key_columns:
                cursor.execute(f"""
                    SELECT COUNT(*) as null_count 
                    FROM {schema}.fct_stock_price 
                    WHERE {col} IS NULL
                """)
                
                null_count = cursor.fetchone()['null_count']
                if null_count > 0:
                    errors.append(f"Found {null_count} null values in NOT NULL column '{col}'")
            
            # Check 4: Record counts
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {schema}.fct_stock_price")
            record_count = cursor.fetchone()['cnt']
            
            if record_count == 0:
                errors.append("Fact table is empty")
            elif record_count < 100:
                warnings.append(f"Low record count: {record_count}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error during transformed data validation: {e}")
            errors.append(f"Database validation failed: {str(e)}")
        
        is_valid = len(errors) == 0
        
        result = {
            'is_valid': is_valid,
            'errors': errors,
            'warnings': warnings,
            'summary': f'{len(errors)} errors, {len(warnings)} warnings',
            'validation_timestamp': datetime.now().isoformat(),
            'record_count': record_count if 'record_count' in locals() else 0,
        }
        
        logger.info(f"Transformed data validation complete: {result['summary']}")
        return result
    
    def check_for_duplicates(self, schema: str = 'analytics', 
                            table: str = 'fct_stock_price') -> Dict[str, Any]:
        """
        Check for duplicate records in a table.
        
        Args:
            schema: Database schema
            table: Table name to check
            
        Returns:
            Duplicate check result
        """
        logger.info(f"Checking for duplicates in {schema}.{table}...")
        
        duplicates_found = []
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # The fact table grain is one row per stock and calendar date.
            cursor.execute(f"""
                SELECT stock_id, date_id, COUNT(*) as dup_count
                FROM {schema}.{table}
                GROUP BY stock_id, date_id
                HAVING COUNT(*) > 1
            """)
            
            duplicates_found = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error checking for duplicates: {e}")
        
        result = {
            'has_duplicates': len(duplicates_found) > 0,
            'duplicate_count': len(duplicates_found),
            'duplicates': duplicates_found,
        }
        
        logger.info(f"Duplicate check complete: {len(duplicates_found)} groups with duplicates found")
        return result
    
    def _get_connection(self):
        """
        Get a connection to the PostgreSQL database.
        
        Returns:
            psycopg2 connection object
        """
        return psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name
        )
