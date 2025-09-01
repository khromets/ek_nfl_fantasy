"""
Rate limiting utilities for respectful API and web scraping
"""
import time
import logging
from typing import Optional, Dict
from functools import wraps
from datetime import datetime, timedelta

class RateLimiter:
    """
    Rate limiter for API calls and web scraping with per-domain tracking
    """
    
    def __init__(self):
        self.last_request_times: Dict[str, datetime] = {}
        self.logger = logging.getLogger(__name__)
    
    def wait_if_needed(self, domain: str, min_interval: float):
        """
        Wait if needed to respect rate limits
        
        Args:
            domain: Domain or identifier for rate limiting
            min_interval: Minimum seconds between requests for this domain
        """
        current_time = datetime.now()
        
        if domain in self.last_request_times:
            time_since_last = current_time - self.last_request_times[domain]
            required_wait = min_interval - time_since_last.total_seconds()
            
            if required_wait > 0:
                self.logger.debug(f"Rate limiting: waiting {required_wait:.2f}s for {domain}")
                time.sleep(required_wait)
        
        self.last_request_times[domain] = datetime.now()
    
    def get_delay_for_domain(self, domain: str) -> float:
        """
        Get the delay needed before next request to domain
        
        Args:
            domain: Domain to check
            
        Returns:
            Seconds to wait before next request (0 if no wait needed)
        """
        if domain not in self.last_request_times:
            return 0.0
        
        time_since_last = datetime.now() - self.last_request_times[domain]
        return max(0.0, 1.0 - time_since_last.total_seconds())


def rate_limited(domain: str, interval: float):
    """
    Decorator for rate-limited functions
    
    Args:
        domain: Domain or identifier for rate limiting
        interval: Minimum seconds between calls for this domain
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not hasattr(wrapper, 'rate_limiter'):
                wrapper.rate_limiter = RateLimiter()
            
            wrapper.rate_limiter.wait_if_needed(domain, interval)
            return func(*args, **kwargs)
        return wrapper
    return decorator


class AdaptiveRateLimiter:
    """
    Rate limiter that adapts to server responses and implements exponential backoff
    """
    
    def __init__(self, base_interval: float = 1.0, max_interval: float = 60.0):
        self.base_interval = base_interval
        self.max_interval = max_interval
        self.current_intervals: Dict[str, float] = {}
        self.consecutive_errors: Dict[str, int] = {}
        self.last_request_times: Dict[str, datetime] = {}
        self.logger = logging.getLogger(__name__)
    
    def wait_for_request(self, domain: str, success: bool = True, 
                        status_code: Optional[int] = None):
        """
        Wait appropriate time based on previous request success/failure
        
        Args:
            domain: Domain or identifier
            success: Whether the last request was successful
            status_code: HTTP status code if applicable
        """
        current_interval = self.current_intervals.get(domain, self.base_interval)
        
        # Handle rate limit responses
        if status_code in [429, 503, 502]:  # Rate limited or server error
            self.consecutive_errors[domain] = self.consecutive_errors.get(domain, 0) + 1
            # Exponential backoff
            current_interval = min(
                self.base_interval * (2 ** self.consecutive_errors[domain]),
                self.max_interval
            )
            self.logger.warning(f"Server error {status_code} for {domain}, backing off to {current_interval}s")
        
        elif success and status_code in [200, 201]:
            # Successful request - gradually reduce interval if it was increased
            if domain in self.consecutive_errors and self.consecutive_errors[domain] > 0:
                self.consecutive_errors[domain] = max(0, self.consecutive_errors[domain] - 1)
                current_interval = self.base_interval * (2 ** self.consecutive_errors[domain])
        
        elif not success:
            # Other failure - slight backoff
            self.consecutive_errors[domain] = self.consecutive_errors.get(domain, 0) + 1
            current_interval = min(current_interval * 1.5, self.max_interval)
        
        # Store current interval for domain
        self.current_intervals[domain] = current_interval
        
        # Wait if needed
        current_time = datetime.now()
        if domain in self.last_request_times:
            time_since_last = current_time - self.last_request_times[domain]
            required_wait = current_interval - time_since_last.total_seconds()
            
            if required_wait > 0:
                self.logger.debug(f"Adaptive rate limiting: waiting {required_wait:.2f}s for {domain}")
                time.sleep(required_wait)
        
        self.last_request_times[domain] = datetime.now()
    
    def reset_domain(self, domain: str):
        """Reset rate limiting state for a domain"""
        self.current_intervals.pop(domain, None)
        self.consecutive_errors.pop(domain, None)
        self.last_request_times.pop(domain, None)
        self.logger.info(f"Reset rate limiting for domain: {domain}")


class RequestTimer:
    """
    Context manager for timing and rate limiting requests
    """
    
    def __init__(self, rate_limiter: RateLimiter, domain: str, interval: float):
        self.rate_limiter = rate_limiter
        self.domain = domain
        self.interval = interval
        self.start_time = None
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        self.rate_limiter.wait_if_needed(self.domain, self.interval)
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            self.logger.debug(f"Request to {self.domain} took {duration:.2f}s")


# Global rate limiter instances
_global_rate_limiter = RateLimiter()
_adaptive_rate_limiter = AdaptiveRateLimiter()

def get_rate_limiter() -> RateLimiter:
    """Get global rate limiter instance"""
    return _global_rate_limiter

def get_adaptive_rate_limiter() -> AdaptiveRateLimiter:
    """Get global adaptive rate limiter instance"""
    return _adaptive_rate_limiter