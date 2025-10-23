"""
CoinGecko API Client for Cryptocurrency Data Collection.

This module provides a robust client for collecting cryptocurrency market data
from the CoinGecko API with built-in rate limiting and CSV export capabilities.

Features:
- Real-time crypto prices and market data
- Social sentiment metrics
- Developer activity tracking
- Automatic rate limiting (50 calls/min free tier)
- CSV export with proper schemas
- Data validation

Authors: Data Delta Force
Created: October 2025
"""

import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import time

from .rate_limiter import RateLimiter
from .data_validator import DataValidator

logger = logging.getLogger(__name__)


class CoinGeckoAPIError(Exception):
    """Base exception for CoinGecko API errors."""
    pass


class CoinGeckoClient:
    """
    Client for CoinGecko API v3.
    
    Provides access to cryptocurrency data including:
    - Real-time and historical prices
    - Market capitalization and trading volumes
    - Social sentiment metrics
    - Developer activity
    - Exchange data
    
    Attributes:
        api_key: CoinGecko API key (optional, for Pro tier)
        base_url: API base URL
        tier: 'free' or 'pro'
    """
    
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    # Rate limits (free tier)
    FREE_CALLS_PER_MINUTE = 50
    PRO_CALLS_PER_MINUTE = 500
    
    # Top cryptocurrencies by market cap (as per proposal: BTC, ETH, top 10)
    TOP_COINS = [
        'bitcoin', 'ethereum', 'tether', 'binancecoin', 'ripple',
        'cardano', 'dogecoin', 'solana', 'polkadot', 'matic-network'
    ]
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        validate_data: bool = True,
        tier: str = 'free'
    ):
        """
        Initialize CoinGecko API client.
        
        Args:
            api_key: CoinGecko API key (optional for free tier)
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            validate_data: Whether to validate API responses
            tier: 'free' or 'pro' tier
            
        Example:
            >>> client = CoinGeckoClient()
            >>> btc_data = client.get_coin_data('bitcoin')
        """
        self.api_key = api_key
        self.timeout = timeout
        self.validate_data = validate_data
        self.tier = tier
        
        # Set rate limit based on tier
        calls_per_minute = (
            self.PRO_CALLS_PER_MINUTE if tier == 'pro' 
            else self.FREE_CALLS_PER_MINUTE
        )
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(
            calls_per_minute=calls_per_minute,
            max_retries=max_retries
        )
        
        # Initialize data validator
        self.validator = DataValidator(strict_mode=False)
        
        # Configure session
        self.session = self._create_session(max_retries)
        
        logger.info(
            f"CoinGeckoClient initialized ({tier} tier, "
            f"Rate Limit: {calls_per_minute}/min)"
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
        
        headers = {
            "Accept": "application/json",
            "User-Agent": "DataDeltaForce-MacroCrypto/1.0"
        }
        
        if self.api_key:
            if self.tier == 'pro':
                headers["x-cg-pro-api-key"] = self.api_key
            else:  # free/demo tier
                headers["x-cg-demo-api-key"] = self.api_key
        
        session.headers.update(headers)
        
        return session
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], List[Any]]:
        """
        Make rate-limited API request.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            JSON response
            
        Raises:
            CoinGeckoAPIError: If request fails
        """
        url = f"{self.BASE_URL}/{endpoint}"
        
        if params is None:
            params = {}
        
        try:
            with self.rate_limiter:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    return self._make_request(endpoint, params)
                
                response.raise_for_status()
                
                data = response.json()
                
                return data
                
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {url}")
            raise CoinGeckoAPIError(f"Request timeout: {url}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            raise CoinGeckoAPIError(f"Request failed: {str(e)}")
    
    def get_coin_data(
        self,
        coin_id: str,
        localization: bool = False,
        tickers: bool = True,
        market_data: bool = True,
        community_data: bool = True,
        developer_data: bool = True
    ) -> Dict[str, Any]:
        """
        Get comprehensive data for a specific cryptocurrency.
        
        Args:
            coin_id: CoinGecko coin ID (e.g., 'bitcoin', 'ethereum')
            localization: Include localized languages
            tickers: Include ticker data
            market_data: Include market data
            community_data: Include community/social data
            developer_data: Include developer activity data
            
        Returns:
            Dictionary with comprehensive coin data
            
        Example:
            >>> client = CoinGeckoClient()
            >>> btc = client.get_coin_data('bitcoin')
            >>> print(f"BTC Price: ${btc['market_data']['current_price']['usd']}")
        """
        endpoint = f"coins/{coin_id}"
        
        params = {
            'localization': str(localization).lower(),
            'tickers': str(tickers).lower(),
            'market_data': str(market_data).lower(),
            'community_data': str(community_data).lower(),
            'developer_data': str(developer_data).lower()
        }
        
        data = self._make_request(endpoint, params)
        
        # Validate if enabled
        if self.validate_data:
            self.validator.validate_crypto_data(data)
        
        logger.info(f"Retrieved data for {coin_id}")
        return data
    
    def get_coins_markets(
        self,
        vs_currency: str = 'usd',
        ids: Optional[List[str]] = None,
        order: str = 'market_cap_desc',
        per_page: int = 100,
        page: int = 1,
        price_change_percentage: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get market data for multiple coins.
        
        Args:
            vs_currency: Target currency (usd, eur, etc.)
            ids: List of coin IDs (None for all coins)
            order: Sort order
            per_page: Results per page (max 250)
            page: Page number
            price_change_percentage: Time periods for price change (24h, 7d, 14d, 30d, etc.)
            
        Returns:
            List of dictionaries with market data
            
        Example:
            >>> client = CoinGeckoClient()
            >>> markets = client.get_coins_markets(ids=['bitcoin', 'ethereum'])
        """
        endpoint = "coins/markets"
        
        params = {
            'vs_currency': vs_currency,
            'order': order,
            'per_page': per_page,
            'page': page,
            'price_change_percentage': price_change_percentage
        }
        
        if ids:
            params['ids'] = ','.join(ids)

        # Only add price_change_percentage if explicitly provided
        if price_change_percentage:
            params['price_change_percentage'] = price_change_percentage
        
        data = self._make_request(endpoint, params)
        
        logger.info(f"Retrieved market data for {len(data)} coins")
        return data
    
    def get_top_coins_market_data(
        self,
        vs_currency: str = 'usd',
        top_n: int = 10
    ) -> pd.DataFrame:
        """
        Get market data for top N cryptocurrencies.
        
        Args:
            vs_currency: Target currency
            top_n: Number of top coins to retrieve
            
        Returns:
            DataFrame with top coins market data
            
        Example:
            >>> client = CoinGeckoClient()
            >>> top_10 = client.get_top_coins_market_data(top_n=10)
        """
        data = self.get_coins_markets(
            vs_currency=vs_currency,
            per_page=top_n,
            page=1,
            order='market_cap_desc'
        )
        
        df = self._process_market_data(data)
        
        logger.info(f"Retrieved top {top_n} coins market data")
        return df
    
    def get_historical_prices(
        self,
        coin_id: str,
        vs_currency: str = 'usd',
        days: Union[int, str] = 30,
        interval: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get historical price data for a cryptocurrency.
        
        Args:
            coin_id: CoinGecko coin ID
            vs_currency: Target currency
            days: Number of days (1, 7, 14, 30, 90, 180, 365, max)
            interval: Data interval (daily, hourly) - auto if None
            
        Returns:
            DataFrame with timestamp, price, market_cap, total_volume
            
        Example:
            >>> client = CoinGeckoClient()
            >>> btc_history = client.get_historical_prices('bitcoin', days=90)
        """
        endpoint = f"coins/{coin_id}/market_chart"
        
        params = {
            'vs_currency': vs_currency,
            'days': days
        }
        
        if interval:
            params['interval'] = interval
        
        data = self._make_request(endpoint, params)
        
        # Convert to DataFrame
        df = pd.DataFrame({
            'timestamp': [x[0] for x in data['prices']],
            'price': [x[1] for x in data['prices']],
            'market_cap': [x[1] for x in data['market_caps']],
            'total_volume': [x[1] for x in data['total_volumes']]
        })
        
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['coin_id'] = coin_id
        df['vs_currency'] = vs_currency
        
        logger.info(f"Retrieved {len(df)} historical price points for {coin_id}")
        return df
    
    def get_multiple_coins_snapshot(
        self,
        coin_ids: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Get current market snapshot for multiple coins.
        
        Args:
            coin_ids: List of coin IDs (uses TOP_COINS if None)
            
        Returns:
            DataFrame with current market data for all coins
            
        Example:
            >>> client = CoinGeckoClient()
            >>> snapshot = client.get_multiple_coins_snapshot()
        """
        if coin_ids is None:
            coin_ids = self.TOP_COINS
        
        data = self.get_coins_markets(ids=coin_ids)
        df = self._process_market_data(data)
        
        logger.info(f"Retrieved snapshot for {len(coin_ids)} coins")
        return df
    
    def _process_market_data(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Process market data into standardized DataFrame.
        
        Args:
            data: Raw market data from API
            
        Returns:
            Processed DataFrame with standard schema
        """
        if not data:
            return pd.DataFrame()
        
        # Extract relevant fields
        processed_data = []
        
        for item in data:
            processed_data.append({
                'timestamp': datetime.utcnow(),
                'coin_id': item.get('id'),
                'symbol': item.get('symbol'),
                'name': item.get('name'),
                'current_price_usd': item.get('current_price'),
                'market_cap_usd': item.get('market_cap'),
                'market_cap_rank': item.get('market_cap_rank'),
                'total_volume_usd': item.get('total_volume'),
                'high_24h': item.get('high_24h'),
                'low_24h': item.get('low_24h'),
                'price_change_24h': item.get('price_change_24h'),
                'price_change_24h_pct': item.get('price_change_percentage_24h'),
                'price_change_7d_pct': item.get('price_change_percentage_7d_in_currency'),
                'market_cap_change_24h': item.get('market_cap_change_24h'),
                'market_cap_change_24h_pct': item.get('market_cap_change_percentage_24h'),
                'circulating_supply': item.get('circulating_supply'),
                'total_supply': item.get('total_supply'),
                'max_supply': item.get('max_supply'),
                'ath': item.get('ath'),
                'ath_change_pct': item.get('ath_change_percentage'),
                'ath_date': item.get('ath_date'),
                'atl': item.get('atl'),
                'atl_change_pct': item.get('atl_change_percentage'),
                'atl_date': item.get('atl_date'),
                'last_updated': item.get('last_updated'),
                'data_source': 'coingecko_api'
            })
        
        df = pd.DataFrame(processed_data)
        
        # Convert timestamp columns
        for col in ['last_updated', 'ath_date', 'atl_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    
    def get_rate_limit_stats(self) -> Dict[str, Any]:
        """Get current rate limiter statistics."""
        return self.rate_limiter.get_stats()
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get data validation summary."""
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