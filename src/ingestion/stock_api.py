"""
Stock API Client for fetching market data.

Supports multiple API providers (Alpha Vantage, Finnhub) with fallback logic.
"""

import os
import requests
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)


class StockAPIClient:
    """
    Client for fetching stock market data from external APIs.
    """
    
    def __init__(self, provider: str = 'alpha_vantage'):
        """
        Initialize API client.
        
        Args:
            provider: 'alpha_vantage' or 'finnhub'
        """
        self.provider = provider
        self.api_timeout = 30  # seconds
        
        if provider == 'alpha_vantage':
            self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
            self.base_url = 'https://www.alphavantage.co/query'
            if not self.api_key:
                raise ValueError("ALPHA_VANTAGE_API_KEY not set in environment")
        elif provider == 'finnhub':
            self.api_key = os.getenv('FINNHUB_API_KEY')
            self.base_url = 'https://finnhub.io/api/v1'
            if not self.api_key:
                raise ValueError("FINNHUB_API_KEY not set in environment")
        else:
            raise ValueError(f"Unknown provider: {provider}")
    
    def health_check(self) -> bool:
        """
        Check if API is accessible.
        
        Returns:
            True if API is healthy, raises exception otherwise
        """
        logger.info(f"Performing health check on {self.provider} API")
        
        try:
            if self.provider == 'alpha_vantage':
                params = {
                    'function': 'GLOBAL_QUOTE',
                    'symbol': 'AAPL',
                    'apikey': self.api_key,
                }
                response = requests.get(self.base_url, params=params, timeout=self.api_timeout)
                response.raise_for_status()
                
                data = response.json()
                if 'Global Quote' in data:
                    logger.info("Alpha Vantage API is healthy")
                    return True
                elif 'Error Message' in data:
                    raise Exception(f"API Error: {data['Error Message']}")
                else:
                    logger.warning(f"Unexpected API response: {data}")
                    return True
            
            elif self.provider == 'finnhub':
                response = requests.get(
                    f"{self.base_url}/quote",
                    params={'symbol': 'AAPL', 'token': self.api_key},
                    timeout=self.api_timeout
                )
                response.raise_for_status()
                logger.info("Finnhub API is healthy")
                return True
                
        except requests.exceptions.Timeout:
            logger.error(f"{self.provider} API request timed out")
            raise
        except requests.exceptions.ConnectionError:
            logger.error(f"Failed to connect to {self.provider} API")
            raise
        except Exception as e:
            logger.error(f"API health check failed: {e}")
            raise
    
    def get_daily_data(self, symbol: str, date: Optional[datetime] = None) -> List[Dict]:
        """
        Fetch daily stock data for a symbol.
        
        Args:
            symbol: Stock ticker symbol (e.g., 'AAPL')
            date: Specific date to fetch (uses most recent if not specified)
            
        Returns:
            List of price records
        """
        logger.info(f"Fetching daily data for {symbol}")
        
        try:
            if self.provider == 'alpha_vantage':
                return self._get_daily_data_alpha_vantage(symbol, date)
            elif self.provider == 'finnhub':
                return self._get_daily_data_finnhub(symbol, date)
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            raise
    
    def _get_daily_data_alpha_vantage(self, symbol: str, date: Optional[datetime] = None) -> List[Dict]:
        """
        Fetch daily data from Alpha Vantage API.
        """
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'full',  # Get full history
        }
        
        response = requests.get(self.base_url, params=params, timeout=self.api_timeout)
        response.raise_for_status()
        
        data = response.json()
        
        if 'Error Message' in data:
            raise Exception(f"API Error: {data['Error Message']}")
        
        if 'Time Series (Daily)' not in data:
            logger.warning(f"No time series data found for {symbol}")
            return []
        
        time_series = data['Time Series (Daily)']
        records = []
        
        # Determine which date to fetch (default: most recent available)
        target_date = date.strftime('%Y-%m-%d') if date else None
        
        for date_str, values in time_series.items():
            if target_date and date_str != target_date:
                continue  # Skip if looking for specific date and this isn't it
            
            record = {
                'stock_symbol': symbol,
                'date': date_str,
                'open': float(values['1. open']),
                'high': float(values['2. high']),
                'low': float(values['3. low']),
                'close': float(values['4. close']),
                'volume': int(values['5. volume']),
                'source': 'alpha_vantage',
                'fetched_at': datetime.now().isoformat(),
            }
            records.append(record)
            
            # If looking for specific date, return after finding it
            if target_date:
                break
        
        logger.info(f"Retrieved {len(records)} records for {symbol}")
        return records
    
    def _get_daily_data_finnhub(self, symbol: str, date: Optional[datetime] = None) -> List[Dict]:
        """
        Fetch daily data from Finnhub API.
        """
        # Finnhub uses different endpoint for daily data
        # This would require calling the candles endpoint with daily resolution
        logger.warning("Finnhub implementation requires API key and different endpoint structure")
        raise NotImplementedError("Finnhub integration coming soon")
    
    def get_intraday_data(self, symbol: str) -> List[Dict]:
        """
        Fetch intraday (5-minute interval) stock data.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            List of intraday price records
        """
        logger.info(f"Fetching intraday data for {symbol}")
        
        if self.provider == 'alpha_vantage':
            params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': symbol,
                'interval': '5min',
                'apikey': self.api_key,
                'outputsize': 'compact',  # Return only latest 100 data points
            }
            
            response = requests.get(self.base_url, params=params, timeout=self.api_timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Error Message' in data:
                raise Exception(f"API Error: {data['Error Message']}")
            
            time_series_key = 'Time Series (5min)'
            if time_series_key not in data:
                logger.warning(f"No intraday data found for {symbol}")
                return []
            
            records = []
            for time_str, values in data[time_series_key].items():
                record = {
                    'stock_symbol': symbol,
                    'timestamp': time_str,
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'volume': int(values['5. volume']),
                    'source': 'alpha_vantage',
                    'fetched_at': datetime.now().isoformat(),
                }
                records.append(record)
            
            logger.info(f"Retrieved {len(records)} intraday records for {symbol}")
            return records
        
        raise NotImplementedError(f"Intraday data not implemented for {self.provider}")
    
    def get_multiple_symbols(self, symbols: List[str]) -> Dict[str, List[Dict]]:
        """
        Fetch data for multiple symbols efficiently.
        
        Args:
            symbols: List of stock ticker symbols
            
        Returns:
            Dictionary mapping symbols to their data
        """
        logger.info(f"Fetching data for {len(symbols)} symbols")
        
        results = {}
        for symbol in symbols:
            try:
                results[symbol] = self.get_daily_data(symbol)
            except Exception as e:
                logger.error(f"Failed to fetch data for {symbol}: {e}")
                results[symbol] = []
        
        return results
