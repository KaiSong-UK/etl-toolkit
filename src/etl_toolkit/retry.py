import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    exponential: bool = True,
    exceptions: tuple = (Exception,),
):
    """Decorator that retries a function with exponential backoff."""

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    logger.warning(
                        f"[Retry] {fn.__name__} attempt {attempt}/{max_attempts} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    delay = delay * 2 if exponential else delay + base_delay

        return wrapper

    return decorator
