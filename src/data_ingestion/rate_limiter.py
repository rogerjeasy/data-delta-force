"""
Rate Limiter Implementation for API Clients.

This module provides thread-safe rate limiting functionality with support for:
- Multiple rate limit tiers (per second, per minute, per hour, per day)
- Exponential backoff retry mechanism
- Token bucket algorithm for smooth rate limiting
- Decorator pattern for easy integration

Authors: Data Delta Force
Created: September 2025
"""

import time
import threading
from typing import Optional, Callable, Any
from functools import wraps
from collections import deque
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Thread-safe rate limiter using token bucket algorithm.
    
    Supports multiple time windows (second, minute, hour, day) and provides
    exponential backoff for rate limit violations.
    
    Attributes:
        calls_per_second: Maximum calls allowed per second
        calls_per_minute: Maximum calls allowed per minute
        calls_per_hour: Maximum calls allowed per hour
        calls_per_day: Maximum calls allowed per day
    """
    
    def __init__(
        self,
        calls_per_second: Optional[int] = None,
        calls_per_minute: Optional[int] = None,
        calls_per_hour: Optional[int] = None,
        calls_per_day: Optional[int] = None,
        max_retries: int = 3,
        base_delay: float = 1.0
    ):
        """
        Initialize rate limiter with specified limits.
        
        Args:
            calls_per_second: Max calls per second (None = no limit)
            calls_per_minute: Max calls per minute (None = no limit)
            calls_per_hour: Max calls per hour (None = no limit)
            calls_per_day: Max calls per day (None = no limit)
            max_retries: Maximum retry attempts for rate limit violations
            base_delay: Base delay in seconds for exponential backoff
            
        Example:
            >>> limiter = RateLimiter(calls_per_second=10, calls_per_minute=500)
            >>> with limiter:
            ...     # Your API call here
            ...     response = api_client.get_data()
        """
        self.limits = {
            'second': (calls_per_second, 1),
            'minute': (calls_per_minute, 60),
            'hour': (calls_per_hour, 3600),
            'day': (calls_per_day, 86400)
        }
        
        # Remove None limits
        self.limits = {
            k: v for k, v in self.limits.items() 
            if v[0] is not None
        }
        
        self.max_retries = max_retries
        self.base_delay = base_delay
        
        # Initialize call history for each time window
        self.call_history = {
            period: deque() for period in self.limits.keys()
        }
        
        # Thread lock for thread safety
        self._lock = threading.Lock()
        
        logger.info(
            f"RateLimiter initialized with limits: {self._format_limits()}"
        )
    
    def _format_limits(self) -> str:
        """Format rate limits for logging."""
        return ", ".join([
            f"{calls}/{period}" 
            for period, (calls, _) in self.limits.items()
        ])
    
    def _clean_old_calls(self, period: str, window_seconds: int) -> None:
        """
        Remove calls outside the time window.
        
        Args:
            period: Time period identifier ('second', 'minute', etc.)
            window_seconds: Window size in seconds
        """
        current_time = time.time()
        cutoff_time = current_time - window_seconds
        
        # Remove old timestamps
        while (self.call_history[period] and 
               self.call_history[period][0] < cutoff_time):
            self.call_history[period].popleft()
    
    def _check_rate_limit(self) -> tuple[bool, Optional[float]]:
        """
        Check if rate limit allows a new call.
        
        Returns:
            Tuple of (can_proceed, wait_time)
            - can_proceed: True if call can proceed
            - wait_time: Seconds to wait if rate limited (None if can proceed)
        """
        current_time = time.time()
        max_wait_time = 0.0
        
        for period, (max_calls, window_seconds) in self.limits.items():
            # Clean old calls
            self._clean_old_calls(period, window_seconds)
            
            # Check if limit exceeded
            if len(self.call_history[period]) >= max_calls:
                # Calculate wait time until oldest call expires
                oldest_call = self.call_history[period][0]
                wait_time = (oldest_call + window_seconds) - current_time
                max_wait_time = max(max_wait_time, wait_time)
        
        if max_wait_time > 0:
            return False, max_wait_time
        
        return True, None
    
    def _record_call(self) -> None:
        """Record a new API call timestamp."""
        current_time = time.time()
        for period in self.call_history.keys():
            self.call_history[period].append(current_time)
    
    def acquire(self, blocking: bool = True) -> bool:
        """
        Acquire permission to make an API call.
        
        Args:
            blocking: If True, wait until rate limit allows call.
                     If False, return immediately if rate limited.
        
        Returns:
            True if call permitted, False if rate limited (non-blocking only)
        """
        with self._lock:
            can_proceed, wait_time = self._check_rate_limit()
            
            if can_proceed:
                self._record_call()
                return True
            
            if not blocking:
                logger.warning(f"Rate limit exceeded, would need to wait {wait_time:.2f}s")
                return False
            
            # Blocking mode - wait and retry
            logger.info(f"Rate limit reached, waiting {wait_time:.2f}s")
            time.sleep(wait_time + 0.1)  # Small buffer
            
            # Record call after waiting
            self._record_call()
            return True
    
    def __enter__(self):
        """Context manager entry - acquire rate limit permission."""
        self.acquire(blocking=True)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        return False
    
    def get_stats(self) -> dict[str, int]:
        """
        Get current rate limiter statistics.
        
        Returns:
            Dictionary with call counts per time period
        """
        with self._lock:
            stats = {}
            for period, (max_calls, window_seconds) in self.limits.items():
                self._clean_old_calls(period, window_seconds)
                current_calls = len(self.call_history[period])
                stats[period] = {
                    'current_calls': current_calls,
                    'max_calls': max_calls,
                    'utilization_pct': (current_calls / max_calls * 100)
                }
            return stats
    
    def reset(self) -> None:
        """Reset all rate limit counters."""
        with self._lock:
            for period in self.call_history.keys():
                self.call_history[period].clear()
            logger.info("Rate limiter reset")


def rate_limited(
    calls_per_second: Optional[int] = None,
    calls_per_minute: Optional[int] = None,
    calls_per_hour: Optional[int] = None,
    calls_per_day: Optional[int] = None,
    max_retries: int = 3,
    base_delay: float = 1.0
) -> Callable:
    """
    Decorator to apply rate limiting to functions.
    
    Args:
        calls_per_second: Max calls per second
        calls_per_minute: Max calls per minute
        calls_per_hour: Max calls per hour
        calls_per_day: Max calls per day
        max_retries: Maximum retry attempts
        base_delay: Base delay for exponential backoff
    
    Returns:
        Decorated function with rate limiting
        
    Example:
        >>> @rate_limited(calls_per_minute=60, calls_per_hour=1000)
        ... def fetch_data():
        ...     return api.get_data()
    """
    limiter = RateLimiter(
        calls_per_second=calls_per_second,
        calls_per_minute=calls_per_minute,
        calls_per_hour=calls_per_hour,
        calls_per_day=calls_per_day,
        max_retries=max_retries,
        base_delay=base_delay
    )
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            retry_count = 0
            last_exception = None
            
            while retry_count <= max_retries:
                try:
                    with limiter:
                        return func(*args, **kwargs)
                        
                except Exception as e:
                    last_exception = e
                    retry_count += 1
                    
                    if retry_count > max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}"
                        )
                        raise
                    
                    # Exponential backoff
                    delay = base_delay * (2 ** (retry_count - 1))
                    logger.warning(
                        f"Retry {retry_count}/{max_retries} for {func.__name__} "
                        f"after {delay}s delay. Error: {str(e)}"
                    )
                    time.sleep(delay)
            
            # Should not reach here, but for type safety
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


class ExponentialBackoff:
    """
    Exponential backoff implementation for retry logic.
    
    Provides configurable backoff strategy with jitter to prevent thundering herd.
    """
    
    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True
    ):
        """
        Initialize exponential backoff.
        
        Args:
            base_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            exponential_base: Base for exponential growth
            jitter: Add random jitter to prevent thundering herd
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.attempt = 0
    
    def get_delay(self) -> float:
        """
        Calculate delay for current attempt.
        
        Returns:
            Delay in seconds
        """
        import random
        
        delay = min(
            self.base_delay * (self.exponential_base ** self.attempt),
            self.max_delay
        )
        
        if self.jitter:
            # Add up to 25% random jitter
            delay *= (0.75 + 0.25 * random.random())
        
        self.attempt += 1
        return delay
    
    def reset(self) -> None:
        """Reset attempt counter."""
        self.attempt = 0