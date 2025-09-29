"""
Data Ingestion Module for Macro-Crypto Risk Intelligence Platform.

This module provides API clients for collecting data from various sources:
- CoinGecko API for cryptocurrency market data
- FRED API for US macroeconomic indicators

The module includes rate limiting, data validation, and error handling
capabilities to ensure robust data collection.

Authors: Data Delta Force
Created: September 2025
"""

from .coingecko_client import CoinGeckoClient
from .fred_client import FREDClient
from .rate_limiter import RateLimiter
from .data_validator import DataValidator

__version__ = "0.1.0"
__all__ = [
    "CoinGeckoClient",
    "FREDClient", 
    "RateLimiter",
    "DataValidator"
]