"""
AWS Glue job for transforming data from Bronze to Silver layer.
"""
import json
import sys
from typing import Dict, List, Optional

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from src.bronze.validate import DataValidator
from src.silver.s3tables import S3TablesManager
from src.silver.transform import DataTransformer
from src.utils.logging_utils import get_glue_logger

# Initialize logger
logger = get_glue_logger("bronze_to_silver")


def process_args():
    """Process job arguments."""
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "source_path",
            "table_name",
            "transformations_json",
            "partition_keys",
            "validate_schema",
        ],
    )
    
    # Parse transformations JSON
    transformations = []
    if args["transformations_json"]:
        transformations = json.loads(args["transformations_json"])
    
    # Parse partition keys if provided
    partition_keys = {}
    if args["partition_keys"]:
        for kv_pair in args["partition_keys"].split(","):
            if "=" in kv_pair:
                key, value = kv_pair.split("=", 1)
                partition_keys[key.strip()] = value.strip()
    
    # Parse validate_schema flag
    validate_schema = args["validate_schema"].lower() == "true"
    
    return {
        "job_name": args["JOB_NAME"],
        "source_path": args["source_path"],
        "table_name": args["table_name"],
        "transformations": transformations,
        "partition_keys": partition_keys,
        "validate_schema": validate_schema,
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
        # Initialize components
        data_validator = DataValidator()
        data_transformer = DataTransformer()
        s3tables_manager = S3TablesManager()
        
        # Validate the source data if required
        if args["validate_schema"]:
            validation_results = data_validator.validate_s3_data(args["source_path"])
            if not validation_results.get("schema_validation", {}).get("valid", True):
                logger.error(f"Schema validation failed: {validation_results}")
                raise ValueError("Schema validation failed")
        
        # Transform the data
        transformation_results = data_transformer.transform_s3_data(
            source_s3_path=args["source_path"],
            transformations=args["transformations"]
        )
        
        logger.info(f"Transformation results: {transformation_results}")
        
        # Load the transformed data
        import pandas as pd
        import awswrangler as wr
        
        # Use AWS Data Wrangler to read the transformed data
        df = wr.s3.read_parquet(path=transformation_results["target_path"])
        
        # Write to S3Tables
        write_results = s3tables_manager.write_to_table(
            df=df,
            table_name=args["table_name"],
            partition_values=args["partition_keys"],
            mode="append"
        )
        
        logger.info(f"Successfully wrote data to S3Table: {write_results}")
        
        # Commit the job
        job.commit()
        
        return {
            "status": "success",
            "source_path": args["source_path"],
            "table": f"{s3tables_manager.database_name}.{args['table_name']}",
            "row_count": write_results["row_count"],
        }
    
    except Exception as e:
        logger.error(f"Error in job execution: {str(e)}")
        raise


if __name__ == "__main__":
    run_job()
