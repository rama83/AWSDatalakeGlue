"""
Logging utilities for the AWS Data Lake Framework.
"""
import logging
import os
import sys
from typing import Optional

from config.config import config


def setup_logger(
    name: str, 
    level: Optional[str] = None, 
    log_format: Optional[str] = None
) -> logging.Logger:
    """
    Set up a logger with the specified configuration.

    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Log message format

    Returns:
        Configured logger
    """
    # Get configuration from config if not provided
    if level is None:
        level = config.get("logging", "level", "INFO")
    
    if log_format is None:
        log_format = config.get(
            "logging", 
            "format", 
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    # Create logger
    logger = logging.getLogger(name)
    
    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    
    logger.setLevel(numeric_level)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    console_handler.setFormatter(formatter)
    
    # Add handler to logger if it doesn't already have handlers
    if not logger.handlers:
        logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


def get_glue_logger(job_name: str) -> logging.Logger:
    """
    Get a logger configured for AWS Glue jobs.

    Args:
        job_name: Name of the Glue job

    Returns:
        Configured logger for Glue
    """
    # AWS Glue uses a specific log format
    glue_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Create logger with Glue-specific configuration
    logger = setup_logger(f"glue.{job_name}", log_format=glue_format)
    
    # Add job name as a prefix to all log messages
    for handler in logger.handlers:
        handler.setFormatter(
            logging.Formatter(f"[{job_name}] {glue_format}")
        )
    
    return logger
