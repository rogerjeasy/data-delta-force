"""
FRED (Federal Reserve Economic Data) API Client.

This module provides a robust client for collecting US macroeconomic data
from the Federal Reserve Bank of St. Louis FRED API.

Features:
- Access to 800,000+ economic time series
- Automatic rate limiting (120 calls/min, 120,000/day)
- Data validation and quality checks
- Support for multiple data formats
- Comprehensive error handling

Authors: Data Delta Force
Created: September 2025
"""

import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

from .rate_limiter import RateLimiter
from .data_validator import DataValidator

logger = logging.getLogger(__name__)


class FREDAPIError(Exception):
    """Base exception for FRED API errors."""
    pass


class FREDClient:
    """
    Client for Federal Reserve Economic Data (FRED) API.
    
    Provides access to US macroeconomic indicators including:
    - Federal funds rate
    - Inflation metrics (CPI, PCE)
    - Employment data
    - GDP and economic growth
    - Yield curves and interest rates
    - Money supply
    
    Attributes:
        api_key: FRED API key
        base_url: API base URL
    """
    
    BASE_URL = "https://api.stlouisfed.org/fred"
    
    # Rate limits
    CALLS_PER_MINUTE = 120
    CALLS_PER_DAY = 120000
    
    # Common economic series IDs
    SERIES_IDS = {
        # Interest Rates
        'fed_funds_rate': 'DFF',  # Daily Federal Funds Rate
        'fed_funds_target': 'DFEDTARU',  # Federal Funds Target Rate - Upper Limit
        '10y_treasury': 'DGS10',  # 10-Year Treasury Constant Maturity Rate
        '2y_treasury': 'DGS2',  # 2-Year Treasury
        'real_fed_funds': 'REAINTRATREARAT10Y',  # Real Federal Funds Rate
        
        # Inflation
        'cpi': 'CPIAUCSL',  # Consumer Price Index
        'core_cpi': 'CPILFESL',  # Core CPI (ex food & energy)
        'pce': 'PCE',  # Personal Consumption Expenditures
        'core_pce': 'PCEPILFE',  # Core PCE
        'inflation_expectations': 'T5YIFR',  # 5-Year Breakeven Inflation Rate
        
        # Employment
        'unemployment_rate': 'UNRATE',  # Unemployment Rate
        'nonfarm_payrolls': 'PAYEMS',  # Nonfarm Payrolls
        'labor_force': 'CLF16OV',  # Civilian Labor Force
        'participation_rate': 'CIVPART',  # Labor Force Participation Rate
        'initial_claims': 'ICSA',  # Initial Unemployment Claims
        
        # GDP & Growth
        'gdp': 'GDP',  # Gross Domestic Product
        'real_gdp': 'GDPC1',  # Real GDP
        'gdp_growth': 'A191RL1Q225SBEA',  # Real GDP Growth Rate
        'industrial_production': 'INDPRO',  # Industrial Production Index
        'capacity_utilization': 'TCU',  # Capacity Utilization
        
        # Money Supply
        'm1': 'M1SL',  # M1 Money Stock
        'm2': 'M2SL',  # M2 Money Stock
        'monetary_base': 'BOGMBASE',  # Monetary Base
        
        # Markets
        'vix': 'VIXCLS',  # CBOE Volatility Index
        'sp500': 'SP500',  # S&P 500 Index
        'wilshire5000': 'WILL5000IND',  # Wilshire 5000 Total Market Index
        
        # Housing
        'housing_starts': 'HOUST',  # Housing Starts
        'home_prices': 'CSUSHPISA',  # Case-Shiller Home Price Index
    }
    
    def __init__(
        self,
        api_key: str,
        timeout: int = 30,
        max_retries: int = 3,
        validate_data: bool = True
    ):
        """
        Initialize FRED API client.
        
        Args:
            api_key: FRED API key (get from https://fred.stlouisfed.org/docs/api/api_key.html)
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            validate_data: Whether to validate API responses
            
        Example:
            >>> client = FREDClient(api_key="your_api_key_here")
            >>> cpi_data = client.get_series('CPIAUCSL')
        """
        if not api_key:
            raise ValueError("FRED API key is required")
        
        self.api_key = api_key
        self.timeout = timeout
        self.validate_data = validate_data
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            calls_per_minute=self.CALLS_PER_MINUTE,
            calls_per_day=self.CALLS_PER_DAY,
            max_retries=max_retries
        )
        
        # Initialize data validator
        self.validator = DataValidator(strict_mode=False)
        
        # Configure session
        self.session = self._create_session(max_retries)
        
        logger.info(
            f"FREDClient initialized (Rate Limit: {self.CALLS_PER_MINUTE}/min, "
            f"{self.CALLS_PER_DAY}/day)"
        )
    
    def _create_session(self, max_retries: int) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            "Accept": "application/json",
            "User-Agent": "DataDeltaForce-MacroCrypto/1.0"
        })
        
        return session
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make rate-limited API request.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            JSON response as dictionary
            
        Raises:
            FREDAPIError: If request fails
        """
        url = f"{self.BASE_URL}/{endpoint}"
        
        if params is None:
            params = {}
        
        # Add API key and JSON format
        params['api_key'] = self.api_key
        params['file_type'] = 'json'
        
        try:
            with self.rate_limiter:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                
                data = response.json()
                
                # Check for API errors
                if 'error_code' in data:
                    error_msg = data.get('error_message', 'Unknown error')
                    raise FREDAPIError(f"API Error: {error_msg}")
                
                return data
                
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {url}")
            raise FREDAPIError(f"Request timeout: {url}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            raise FREDAPIError(f"Request failed: {str(e)}")
    
    def get_series(
        self,
        series_id: str,
        observation_start: Optional[Union[str, datetime]] = None,
        observation_end: Optional[Union[str, datetime]] = None,
        frequency: Optional[str] = None,
        aggregation_method: str = 'avg',
        output_type: int = 1
    ) -> pd.DataFrame:
        """
        Get observations for an economic data series.
        
        Args:
            series_id: FRED series ID (e.g., 'CPIAUCSL', 'DFF')
            observation_start: Start date (YYYY-MM-DD or datetime)
            observation_end: End date (YYYY-MM-DD or datetime)
            frequency: Frequency (d, w, bw, m, q, sa, a) - None for native
            aggregation_method: Method for frequency aggregation (avg, sum, eop)
            output_type: 1=observations only, 2=observations with vintage dates
            
        Returns:
            DataFrame with date and value columns
            
        Example:
            >>> client = FREDClient(api_key="your_key")
            >>> cpi = client.get_series('CPIAUCSL', 
            ...     observation_start='2020-01-01',
            ...     observation_end='2024-12-31')
            >>> print(cpi.head())
        """
        endpoint = "series/observations"
        
        params = {
            'series_id': series_id,
            'output_type': output_type
        }
        
        # Convert datetime to string if needed
        if observation_start:
            if isinstance(observation_start, datetime):
                observation_start = observation_start.strftime('%Y-%m-%d')
            params['observation_start'] = observation_start
        
        if observation_end:
            if isinstance(observation_end, datetime):
                observation_end = observation_end.strftime('%Y-%m-%d')
            params['observation_end'] = observation_end
        
        if frequency:
            params['frequency'] = frequency
            params['aggregation_method'] = aggregation_method
        
        data = self._make_request(endpoint, params)
        
        # Convert to DataFrame
        observations = data.get('observations', [])
        if not observations:
            logger.warning(f"No observations returned for series {series_id}")
            return pd.DataFrame()
        
        df = pd.DataFrame(observations)
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Validate data
        if self.validate_data:
            self.validator.validate_time_series(
                df,
                timestamp_col='date',
                value_col='value'
            )
        
        logger.info(f"Retrieved {len(df)} observations for {series_id}")
        return df[['date', 'value']]
    
    def get_series_info(self, series_id: str) -> Dict[str, Any]:
        """
        Get metadata for a data series.
        
        Args:
            series_id: FRED series ID
            
        Returns:
            Dictionary containing series metadata
            
        Example:
            >>> client = FREDClient(api_key="your_key")
            >>> info = client.get_series_info('CPIAUCSL')
            >>> print(info['title'])
        """
        endpoint = "series"
        params = {'series_id': series_id}
        
        data = self._make_request(endpoint, params)
        
        series_list = data.get('seriess', [])
        if series_list:
            logger.info(f"Retrieved info for {series_id}")
            return series_list[0]
        
        raise FREDAPIError(f"Series {series_id} not found")
    
    def search_series(
        self,
        search_text: str,
        limit: int = 100,
        order_by: str = 'popularity',
        sort_order: str = 'desc'
    ) -> List[Dict[str, Any]]:
        """
        Search for economic data series.
        
        Args:
            search_text: Search query
            limit: Maximum number of results
            order_by: Sort field (search_rank, series_id, title, units, 
                     frequency, seasonal_adjustment, realtime_start, 
                     realtime_end, last_updated, observation_start, 
                     observation_end, popularity, group_popularity)
            sort_order: Sort order (asc, desc)
            
        Returns:
            List of matching series
            
        Example:
            >>> client = FREDClient(api_key="your_key")
            >>> results = client.search_series("unemployment rate")
            >>> for series in results[:5]:
            ...     print(f"{series['id']}: {series['title']}")
        """
        endpoint = "series/search"
        
        params = {
            'search_text': search_text,
            'limit': limit,
            'order_by': order_by,
            'sort_order': sort_order
        }
        
        data = self._make_request(endpoint, params)
        
        series_list = data.get('seriess', [])
        logger.info(f"Found {len(series_list)} series for '{search_text}'")
        return series_list
    
    def get_multiple_series(
        self,
        series_ids: List[str],
        observation_start: Optional[Union[str, datetime]] = None,
        observation_end: Optional[Union[str, datetime]] = None,
        frequency: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get data for multiple series and merge into single DataFrame.
        
        Args:
            series_ids: List of FRED series IDs
            observation_start: Start date
            observation_end: End date
            frequency: Frequency for all series
            
        Returns:
            DataFrame with date index and columns for each series
            
        Example:
            >>> client = FREDClient(api_key="your_key")
            >>> series = ['DFF', 'CPIAUCSL', 'UNRATE']
            >>> df = client.get_multiple_series(series, 
            ...     observation_start='2020-01-01')
            >>> print(df.head())
        """
        dfs = []
        
        for series_id in series_ids:
            try:
                df = self.get_series(
                    series_id,
                    observation_start=observation_start,
                    observation_end=observation_end,
                    frequency=frequency
                )
                df = df.rename(columns={'value': series_id})
                df = df.set_index('date')
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to retrieve {series_id}: {str(e)}")
        
        if not dfs:
            logger.error("No series data retrieved")
            return pd.DataFrame()
        
        # Merge all dataframes
        merged_df = dfs[0]
        for df in dfs[1:]:
            merged_df = merged_df.join(df, how='outer')
        
        merged_df = merged_df.reset_index()
        logger.info(f"Retrieved and merged {len(dfs)} series")
        return merged_df
    
    def get_common_series(
        self,
        series_names: Optional[List[str]] = None,
        observation_start: Optional[Union[str, datetime]] = None,
        observation_end: Optional[Union[str, datetime]] = None
    ) -> pd.DataFrame:
        """
        Get data for commonly used economic indicators.
        
        Args:
            series_names: List of series names from SERIES_IDS dict 
                         (None = all common series)
            observation_start: Start date
            observation_end: End date
            
        Returns:
            DataFrame with economic indicators
            
        Example:
            >>> client = FREDClient(api_key="your_key")
            >>> data = client.get_common_series(
            ...     series_names=['fed_funds_rate', 'cpi', 'unemployment_rate'],
            ...     observation_start='2020-01-01'
            ... )
        """
        if series_names is None:
            # Get all common series
            series_ids = list(self.SERIES_IDS.values())
            column_names = list(self.SERIES_IDS.keys())
        else:
            # Get specified series
            series_ids = [self.SERIES_IDS[name] for name in series_names]
            column_names = series_names
        
        df = self.get_multiple_series(
            series_ids,
            observation_start=observation_start,
            observation_end=observation_end
        )
        
        # Rename columns to friendly names
        rename_dict = dict(zip(series_ids, column_names))
        df = df.rename(columns=rename_dict)
        
        return df
    
    def get_categories(self, category_id: int = 0) -> List[Dict[str, Any]]:
        """
        Get FRED categories.
        
        Args:
            category_id: Category ID (0 for root categories)
            
        Returns:
            List of category dictionaries
            
        Example:
            >>> client = FREDClient(api_key="your_key")
            >>> categories = client.get_categories()
        """
        endpoint = "category/children"
        params = {'category_id': category_id}
        
        data = self._make_request(endpoint, params)
        categories = data.get('categories', [])
        
        logger.info(f"Retrieved {len(categories)} categories")
        return categories
    
    def get_category_series(
        self,
        category_id: int,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get series in a category.
        
        Args:
            category_id: Category ID
            limit: Maximum number of series
            
        Returns:
            List of series in category
        """
        endpoint = "category/series"
        params = {
            'category_id': category_id,
            'limit': limit
        }
        
        data = self._make_request(endpoint, params)
        series_list = data.get('seriess', [])
        
        logger.info(f"Retrieved {len(series_list)} series from category {category_id}")
        return series_list
    
    def get_release_dates(
        self,
        release_id: int,
        realtime_start: Optional[str] = None,
        realtime_end: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get release dates for a data release.
        
        Args:
            release_id: Release ID
            realtime_start: Start date (YYYY-MM-DD)
            realtime_end: End date (YYYY-MM-DD)
            
        Returns:
            List of release dates
            
        Example:
            >>> client = FREDClient(api_key="your_key")
            >>> # Employment Situation release
            >>> dates = client.get_release_dates(50)
        """
        endpoint = "release/dates"
        params = {'release_id': release_id}
        
        if realtime_start:
            params['realtime_start'] = realtime_start
        if realtime_end:
            params['realtime_end'] = realtime_end
        
        data = self._make_request(endpoint, params)
        release_dates = data.get('release_dates', [])
        
        logger.info(f"Retrieved {len(release_dates)} release dates")
        return release_dates
    
    def calculate_growth_rate(
        self,
        series_id: str,
        periods: int = 12,
        observation_start: Optional[Union[str, datetime]] = None,
        observation_end: Optional[Union[str, datetime]] = None
    ) -> pd.DataFrame:
        """
        Calculate year-over-year or period-over-period growth rate.
        
        Args:
            series_id: FRED series ID
            periods: Number of periods for growth calculation (12 for YoY monthly)
            observation_start: Start date
            observation_end: End date
            
        Returns:
            DataFrame with date, value, and growth_rate columns
            
        Example:
            >>> client = FREDClient(api_key="your_key")
            >>> cpi_growth = client.calculate_growth_rate('CPIAUCSL', periods=12)
        """
        df = self.get_series(
            series_id,
            observation_start=observation_start,
            observation_end=observation_end
        )
        
        # Calculate percentage change
        df['growth_rate'] = df['value'].pct_change(periods=periods) * 100
        
        logger.info(f"Calculated {periods}-period growth rate for {series_id}")
        return df
    
    def get_latest_observation(self, series_id: str) -> Dict[str, Any]:
        """
        Get the most recent observation for a series.
        
        Args:
            series_id: FRED series ID
            
        Returns:
            Dictionary with latest observation data
            
        Example:
            >>> client = FREDClient(api_key="your_key")
            >>> latest = client.get_latest_observation('DFF')
            >>> print(f"Fed Funds Rate: {latest['value']}%")
        """
        df = self.get_series(series_id)
        
        if df.empty:
            raise FREDAPIError(f"No data available for {series_id}")
        
        latest = df.iloc[-1]
        
        result = {
            'series_id': series_id,
            'date': latest['date'],
            'value': latest['value']
        }
        
        logger.info(f"Retrieved latest observation for {series_id}")
        return result
    
    def get_series_as_dict(
        self,
        series_id: str,
        observation_start: Optional[Union[str, datetime]] = None,
        observation_end: Optional[Union[str, datetime]] = None
    ) -> Dict[str, float]:
        """
        Get series data as dictionary with dates as keys.
        
        Args:
            series_id: FRED series ID
            observation_start: Start date
            observation_end: End date
            
        Returns:
            Dictionary mapping date strings to values
        """
        df = self.get_series(
            series_id,
            observation_start=observation_start,
            observation_end=observation_end
        )
        
        # Convert to dictionary
        df['date_str'] = df['date'].dt.strftime('%Y-%m-%d')
        result = df.set_index('date_str')['value'].to_dict()
        
        return result
    
    def get_rate_limit_stats(self) -> Dict[str, Any]:
        """Get current rate limiter statistics."""
        return self.rate_limiter.get_stats()
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get data validation summary."""
        return self.validator.get_validation_summary()
    
    @classmethod
    def list_common_series(cls) -> Dict[str, str]:
        """
        List all predefined common economic series.
        
        Returns:
            Dictionary mapping series names to FRED IDs
            
        Example:
            >>> common_series = FREDClient.list_common_series()
            >>> for name, series_id in common_series.items():
            ...     print(f"{name}: {series_id}")
        """
        return cls.SERIES_IDS.copy()
    
    def close(self) -> None:
        """Close the session and clean up resources."""
        self.session.close()
        logger.info("FREDClient session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False