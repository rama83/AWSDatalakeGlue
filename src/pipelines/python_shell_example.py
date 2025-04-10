"""
AWS Glue Python Shell job example.
"""
import argparse
import json
import os
import sys
from datetime import datetime

import boto3
import pandas as pd

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.bronze.ingest import BronzeIngestion
from src.utils.logging_utils import setup_logger

# Initialize logger
logger = setup_logger("python_shell_example")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Python Shell job example")
    
    parser.add_argument(
        "--source_path",
        type=str,
        required=True,
        help="Path to the source data"
    )
    
    parser.add_argument(
        "--source_type",
        type=str,
        default="csv",
        choices=["csv", "json", "parquet"],
        help="Type of source data"
    )
    
    parser.add_argument(
        "--target_bucket",
        type=str,
        required=True,
        help="Target S3 bucket for Bronze data"
    )
    
    parser.add_argument(
        "--target_prefix",
        type=str,
        default="data",
        help="Target S3 prefix for Bronze data"
    )
    
    parser.add_argument(
        "--partition_keys",
        type=str,
        default="",
        help="Comma-separated list of partition keys"
    )
    
    return parser.parse_args()


def run_job():
    """Run the Python Shell job."""
    # Parse arguments
    args = parse_args()
    logger.info(f"Starting job with arguments: {args}")
    
    # Parse partition keys if provided
    partition_keys = []
    if args.partition_keys:
        partition_keys = args.partition_keys.split(",")
    
    try:
        # Initialize Bronze ingestion
        bronze_ingestion = BronzeIngestion(
            source_type=args.source_type,
            target_bucket=args.target_bucket,
            target_prefix=args.target_prefix,
        )
        
        # Process the source path
        source_path = args.source_path
        
        # Add metadata
        metadata = {
            "job_type": "python_shell",
            "source_type": args.source_type,
            "timestamp": datetime.now().isoformat()
        }
        
        # Ingest the file
        target_s3_path = bronze_ingestion.ingest_file(
            source_path=source_path,
            partition_keys=partition_keys,
            metadata=metadata,
        )
        
        logger.info(f"Successfully ingested data to {target_s3_path}")
        
        return {
            "status": "success",
            "source_path": source_path,
            "target_path": target_s3_path,
        }
    
    except Exception as e:
        logger.error(f"Error in job execution: {str(e)}")
        raise


if __name__ == "__main__":
    run_job()
