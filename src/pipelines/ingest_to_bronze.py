"""
AWS Glue job for ingesting data to the Bronze layer.
"""
import sys
from typing import Dict, List, Optional

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.bronze.ingest import BronzeIngestion
from src.utils.logging_utils import get_glue_logger

# Initialize logger
logger = get_glue_logger("ingest_to_bronze")


def process_args():
    """Process job arguments."""
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "source_path",
            "source_type",
            "target_bucket",
            "target_prefix",
            "partition_keys",
        ],
    )
    
    # Parse partition keys if provided
    partition_keys = []
    if args["partition_keys"]:
        partition_keys = args["partition_keys"].split(",")
    
    return {
        "job_name": args["JOB_NAME"],
        "source_path": args["source_path"],
        "source_type": args["source_type"],
        "target_bucket": args["target_bucket"],
        "target_prefix": args["target_prefix"],
        "partition_keys": partition_keys,
    }


def run_job():
    """Run the Glue job."""
    # Get job arguments
    args = process_args()
    logger.info(f"Starting job with arguments: {args}")
    
    # Initialize Spark context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["job_name"])
    
    try:
        # Initialize Bronze ingestion
        bronze_ingestion = BronzeIngestion(
            source_type=args["source_type"],
            target_bucket=args["target_bucket"],
            target_prefix=args["target_prefix"],
        )
        
        # Process the source path
        source_path = args["source_path"]
        
        # Add metadata
        metadata = {
            "job_name": args["job_name"],
            "source_type": args["source_type"],
        }
        
        # Ingest the file
        target_s3_path = bronze_ingestion.ingest_file(
            source_path=source_path,
            partition_keys=args["partition_keys"],
            metadata=metadata,
        )
        
        logger.info(f"Successfully ingested data to {target_s3_path}")
        
        # Commit the job
        job.commit()
        
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
