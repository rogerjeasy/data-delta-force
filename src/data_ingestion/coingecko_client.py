"""
CoinGecko API Client for Cryptocurrency Data Collection.

This module provides a robust client for interacting with the CoinGecko API
to collect cryptocurrency market data, social metrics, and historical prices.

Features:
- Automatic rate limiting
- Retry logic with exponential backoff
- Data validation
- Comprehensive error handling
- Support for both free and pro API tiers

Authors: Data Delta Force
Created: September 2025
"""

import logging
import time
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

from .rate_limiter import RateLimiter
from .data_validator import DataValidator, ValidationResult

logger = logging.getLogger(__name__)


class CoinGeckoAPIError(Exception):
    """Base exception for CoinGecko API errors."""
    pass


class RateLimitError(CoinGeckoAPIError):
    """Exception raised when rate limit is exceeded."""
    pass


class CoinGeckoClient:
    """
    Client for CoinGecko API with rate limiting and error handling.
    
    Supports both free and pro API tiers with automatic rate limit management.
    Includes data validation and comprehensive logging.
    
    Attributes:
        api_key: CoinGecko API key (optional for free tier)
        is_pro: Whether using pro API tier
        base_url: API base URL
    """
    
    # API endpoints
    BASE_URL_FREE = "https://api.coingecko.com/api/v3"
    BASE_URL_PRO = "https://pro-api.coingecko.com/api/v3"
    
    # Rate limits (calls per minute)
    RATE_LIMIT_FREE = 30
    RATE_LIMIT_PRO = 500
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        is_pro: bool = False,
        timeout: int = 30,
        max_retries: int = 3,
        validate_data: bool = True
    ):
        """
        Initialize CoinGecko API client.
        
        Args:
            api_key: API key for authentication (Demo or Pro key)
            is_pro: Whether using pro API tier (set False for Demo keys)
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            validate_data: Whether to validate API responses
            
        Note:
            - No API key (free tier): Use api.coingecko.com, 50 calls/min
            - Demo API key: Use api.coingecko.com, 50 calls/min
            - Pro API key: Use pro-api.coingecko.com, 500 calls/min
            
        Example:
            >>> # Free tier (no key)
            >>> client = CoinGeckoClient()
            >>> 
            >>> # Demo key (from free account)
            >>> client = CoinGeckoClient(api_key="demo_key", is_pro=False)
            >>> 
            >>> # Pro key
            >>> client = CoinGeckoClient(api_key="pro_key", is_pro=True)
        """
        self.api_key = api_key
        self.is_pro = is_pro
        self.timeout = timeout
        self.validate_data = validate_data
        
        # Demo keys and free tier both use the standard endpoint
        # Only true Pro keys use the pro endpoint
        if is_pro and api_key:
            self.base_url = self.BASE_URL_PRO
            rate_limit = self.RATE_LIMIT_PRO
        else:
            self.base_url = self.BASE_URL_FREE
            rate_limit = self.RATE_LIMIT_FREE
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            calls_per_minute=rate_limit,
            calls_per_hour=rate_limit * 60,
            max_retries=max_retries
        )
        
        # Initialize data validator
        self.validator = DataValidator(strict_mode=False)
        
        # Configure session with retry strategy
        self.session = self._create_session(max_retries)
        
        tier = "Pro" if (is_pro and api_key) else ("Demo" if api_key else "Free")
        logger.info(
            f"CoinGeckoClient initialized (Tier: {tier}, "
            f"Rate Limit: {rate_limit}/min, URL: {self.base_url})"
        )
    
    def _create_session(self, max_retries: int) -> requests.Session:
        """
        Create requests session with retry strategy.
        
        Args:
            max_retries: Maximum number of retries
            
        Returns:
            Configured requests Session
        """
        session = requests.Session()
        
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        headers = {
            "Accept": "application/json",
            "User-Agent": "DataDeltaForce-MacroCrypto/1.0"
        }
        
        # Add API key to headers based on tier
        if self.api_key:
            if self.is_pro:
                # Pro API uses x-cg-pro-api-key header
                headers["x-cg-pro-api-key"] = self.api_key
            else:
                # Demo API uses x-cg-demo-api-key header
                headers["x-cg-demo-api-key"] = self.api_key
        
        session.headers.update(headers)
        
        return session
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make rate-limited API request.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response as dictionary
            
        Raises:
            CoinGeckoAPIError: If request fails
            RateLimitError: If rate limit exceeded
        """
        url = f"{self.base_url}{endpoint}"
        
        if params is None:
            params = {}
        
        # API key is now in headers (set during session creation)
        # No need to add it to query parameters
        
        try:
            with self.rate_limiter:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                
                # Check for rate limit
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limit hit, retry after {retry_after}s")
                    raise RateLimitError(f"Rate limit exceeded, retry after {retry_after}s")
                
                # Check for authentication errors
                if response.status_code == 401:
                    logger.error("Authentication failed - check API key")
                    raise CoinGeckoAPIError("Invalid API key or authentication failed")
                
                # Check for bad request with detailed error message
                if response.status_code == 400:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('status', {}).get('error_message', 'Bad Request')
                        logger.error(f"Bad request: {error_msg}")
                        raise CoinGeckoAPIError(f"Bad request: {error_msg}")
                    except:
                        logger.error(f"Bad request: {response.text[:200]}")
                        raise CoinGeckoAPIError(f"Bad request - check API parameters")
                
                # Raise for other HTTP errors
                response.raise_for_status()
                
                return response.json()
                
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {url}")
            raise CoinGeckoAPIError(f"Request timeout: {url}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            raise CoinGeckoAPIError(f"Request failed: {str(e)}")
    
    def get_coin_data(
        self,
        coin_id: str,
        include_market_data: bool = True,
        include_community_data: bool = True,
        include_developer_data: bool = False
    ) -> Dict[str, Any]:
        """
        Get comprehensive data for a specific cryptocurrency.
        
        Args:
            coin_id: CoinGecko coin ID (e.g., 'bitcoin', 'ethereum')
            include_market_data: Include price and market data
            include_community_data: Include social/community metrics
            include_developer_data: Include developer activity metrics
            
        Returns:
            Dictionary containing coin data
            
        Example:
            >>> client = CoinGeckoClient()
            >>> btc_data = client.get_coin_data("bitcoin")
            >>> print(f"BTC Price: ${btc_data['market_data']['current_price']['usd']}")
        """
        endpoint = f"/coins/{coin_id}"
        
        params = {
            "localization": "false",
            "tickers": "false",
            "market_data": str(include_market_data).lower(),
            "community_data": str(include_community_data).lower(),
            "developer_data": str(include_developer_data).lower(),
            "sparkline": "false"
        }
        
        data = self._make_request(endpoint, params)
        
        # Validate data if enabled
        if self.validate_data and include_market_data:
            market_data = data.get('market_data', {})
            if market_data:
                self.validator.validate_crypto_data({
                    'id': data.get('id'),
                    'symbol': data.get('symbol'),
                    'name': data.get('name'),
                    'current_price': market_data.get('current_price', {}).get('usd'),
                    'market_cap': market_data.get('market_cap', {}).get('usd'),
                    'total_volume': market_data.get('total_volume', {}).get('usd'),
                    'last_updated': data.get('last_updated')
                })
        
        logger.info(f"Retrieved data for {coin_id}")
        return data
    
    def get_coin_market_data(
        self,
        coin_ids: Optional[List[str]] = None,
        vs_currency: str = "usd",
        order: str = "market_cap_desc",
        per_page: int = 100,
        page: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Get market data for multiple cryptocurrencies.
        
        Args:
            coin_ids: List of coin IDs to fetch (None for all)
            vs_currency: Currency for prices (default: USD)
            order: Sort order (market_cap_desc, volume_desc, etc.)
            per_page: Results per page (max 250)
            page: Page number
            
        Returns:
            List of dictionaries containing market data
            
        Example:
            >>> client = CoinGeckoClient()
            >>> top_coins = client.get_coin_market_data(per_page=10)
            >>> for coin in top_coins:
            ...     print(f"{coin['name']}: ${coin['current_price']}")
        """
        endpoint = "/coins/markets"
        
        params = {
            "vs_currency": vs_currency,
            "order": order,
            "per_page": min(per_page, 250),
            "page": page,
            "sparkline": "false",
            "price_change_percentage": "24h,7d"
        }
        
        if coin_ids:
            params["ids"] = ",".join(coin_ids)
        
        data = self._make_request(endpoint, params)
        
        # Validate each coin's data
        if self.validate_data:
            for coin in data:
                self.validator.validate_crypto_data(coin)
        
        logger.info(f"Retrieved market data for {len(data)} coins")
        return data
    
    def get_coin_history(
        self,
        coin_id: str,
        date: Union[str, datetime],
        localization: bool = False
    ) -> Dict[str, Any]:
        """
        Get historical data for a specific date.
        
        Args:
            coin_id: CoinGecko coin ID
            date: Date string (dd-mm-yyyy) or datetime object
            localization: Include localized data
            
        Returns:
            Dictionary containing historical data
            
        Example:
            >>> client = CoinGeckoClient()
            >>> historic = client.get_coin_history("bitcoin", "01-01-2024")
        """
        endpoint = f"/coins/{coin_id}/history"
        
        if isinstance(date, datetime):
            date = date.strftime("%d-%m-%Y")
        
        params = {
            "date": date,
            "localization": str(localization).lower()
        }
        
        data = self._make_request(endpoint, params)
        logger.info(f"Retrieved historical data for {coin_id} on {date}")
        return data
    
    def get_coin_market_chart(
        self,
        coin_id: str,
        vs_currency: str = "usd",
        days: Union[int, str] = 30,
        interval: Optional[str] = None
    ) -> Dict[str, List]:
        """
        Get historical market data (price, volume, market cap).
        
        Args:
            coin_id: CoinGecko coin ID
            vs_currency: Currency for prices
            days: Number of days (1, 7, 14, 30, 90, 180, 365, max)
            interval: Data interval (daily, hourly)
            
        Returns:
            Dictionary with prices, market_caps, and total_volumes arrays
            
        Example:
            >>> client = CoinGeckoClient()
            >>> chart_data = client.get_coin_market_chart("bitcoin", days=7)
            >>> df = pd.DataFrame(chart_data['prices'], columns=['timestamp', 'price'])
        """
        endpoint = f"/coins/{coin_id}/market_chart"
        
        params = {
            "vs_currency": vs_currency,
            "days": str(days)
        }
        
        if interval:
            params["interval"] = interval
        
        data = self._make_request(endpoint, params)
        logger.info(f"Retrieved market chart for {coin_id} ({days} days)")
        return data
    
    def get_coin_market_chart_range(
        self,
        coin_id: str,
        vs_currency: str = "usd",
        from_timestamp: Union[int, datetime] = None,
        to_timestamp: Union[int, datetime] = None
    ) -> Dict[str, List]:
        """
        Get historical market data within a time range.
        
        Args:
            coin_id: CoinGecko coin ID
            vs_currency: Currency for prices
            from_timestamp: Start timestamp (Unix) or datetime
            to_timestamp: End timestamp (Unix) or datetime
            
        Returns:
            Dictionary with historical data
            
        Example:
            >>> from datetime import datetime
            >>> client = CoinGeckoClient()
            >>> start = datetime(2024, 1, 1)
            >>> end = datetime(2024, 12, 31)
            >>> data = client.get_coin_market_chart_range("bitcoin", 
            ...     from_timestamp=start, to_timestamp=end)
        """
        endpoint = f"/coins/{coin_id}/market_chart/range"
        
        # Convert datetime to Unix timestamp
        if isinstance(from_timestamp, datetime):
            from_timestamp = int(from_timestamp.timestamp())
        if isinstance(to_timestamp, datetime):
            to_timestamp = int(to_timestamp.timestamp())
        
        params = {
            "vs_currency": vs_currency,
            "from": from_timestamp,
            "to": to_timestamp
        }
        
        data = self._make_request(endpoint, params)
        logger.info(f"Retrieved market chart range for {coin_id}")
        return data
    
    def get_trending_coins(self) -> Dict[str, Any]:
        """
        Get trending coins.
        
        Returns:
            Dictionary containing trending coins data
            
        Example:
            >>> client = CoinGeckoClient()
            >>> trending = client.get_trending_coins()
            >>> for coin in trending['coins']:
            ...     print(coin['item']['name'])
        """
        endpoint = "/search/trending"
        data = self._make_request(endpoint)
        logger.info("Retrieved trending coins")
        return data
    
    def get_global_data(self) -> Dict[str, Any]:
        """
        Get global cryptocurrency market data.
        
        Returns:
            Dictionary containing global market statistics
            
        Example:
            >>> client = CoinGeckoClient()
            >>> global_data = client.get_global_data()
            >>> total_mcap = global_data['data']['total_market_cap']['usd']
            >>> print(f"Total Market Cap: ${total_mcap:,.0f}")
        """
        endpoint = "/global"
        data = self._make_request(endpoint)
        logger.info("Retrieved global market data")
        return data
    
    def search_coins(self, query: str) -> Dict[str, List]:
        """
        Search for coins by name or symbol.
        
        Args:
            query: Search query
            
        Returns:
            Dictionary containing search results
            
        Example:
            >>> client = CoinGeckoClient()
            >>> results = client.search_coins("bitcoin")
        """
        endpoint = "/search"
        params = {"query": query}
        data = self._make_request(endpoint, params)
        logger.info(f"Search completed for query: {query}")
        return data
    
    def get_rate_limit_stats(self) -> Dict[str, Any]:
        """
        Get current rate limiter statistics.
        
        Returns:
            Dictionary with rate limit usage statistics
        """
        return self.rate_limiter.get_stats()
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get data validation summary.
        
        Returns:
            Dictionary with validation statistics
        """
        return self.validator.get_validation_summary()
    
    def close(self) -> None:
        """Close the session and clean up resources."""
        self.session.close()
        logger.info("CoinGeckoClient session closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False