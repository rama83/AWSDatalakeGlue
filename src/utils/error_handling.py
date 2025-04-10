"""
Error handling utilities for the AWS Data Lake Framework.
"""
import functools
import logging
import traceback
from typing import Any, Callable, Dict, Optional, Type, TypeVar, cast

from src.utils.logging_utils import setup_logger

# Type variables for function signatures
F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")

# Set up logger
logger = setup_logger(__name__)


class DataLakeError(Exception):
    """Base exception for all data lake errors."""
    pass


class ConfigurationError(DataLakeError):
    """Error in configuration."""
    pass


class S3Error(DataLakeError):
    """Error in S3 operations."""
    pass


class GlueError(DataLakeError):
    """Error in Glue operations."""
    pass


class ValidationError(DataLakeError):
    """Error in data validation."""
    pass


class TransformationError(DataLakeError):
    """Error in data transformation."""
    pass


def handle_exceptions(
    error_types: Optional[Dict[Type[Exception], str]] = None,
    default_message: str = "An error occurred",
    reraise: bool = True,
    log_traceback: bool = True,
) -> Callable[[F], F]:
    """
    Decorator to handle exceptions in a consistent way.

    Args:
        error_types: Dictionary mapping exception types to error messages
        default_message: Default error message for uncaught exceptions
        reraise: Whether to re-raise the exception after handling
        log_traceback: Whether to log the traceback

    Returns:
        Decorated function
    """
    if error_types is None:
        error_types = {}

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Get the error message based on the exception type
                error_message = default_message
                for error_type, message in error_types.items():
                    if isinstance(e, error_type):
                        error_message = message
                        break

                # Log the error
                if log_traceback:
                    logger.error(
                        f"{error_message}: {str(e)}\n{traceback.format_exc()}"
                    )
                else:
                    logger.error(f"{error_message}: {str(e)}")

                # Re-raise the exception if required
                if reraise:
                    raise

        return cast(F, wrapper)

    return decorator


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable[[F], F]:
    """
    Retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff_factor: Backoff factor for exponential delay
        exceptions: Tuple of exceptions to catch and retry

    Returns:
        Decorated function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            import time
            
            attempts = 0
            current_delay = delay
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempts += 1
                    if attempts >= max_attempts:
                        logger.error(
                            f"Failed after {max_attempts} attempts: {str(e)}"
                        )
                        raise
                    
                    logger.warning(
                        f"Attempt {attempts} failed: {str(e)}. "
                        f"Retrying in {current_delay:.2f} seconds..."
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff_factor
            
            # This should never be reached due to the raise in the except block
            return None
        
        return cast(F, wrapper)
    
    return decorator
