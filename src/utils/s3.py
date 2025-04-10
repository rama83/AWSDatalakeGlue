"""
S3 utilities for the AWS Data Lake Framework.
"""
import os
from typing import Any, Dict, List, Optional, Union

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from config.config import config
from src.utils.error_handling import S3Error, handle_exceptions, retry
from src.utils.logging_utils import setup_logger

# Set up logger
logger = setup_logger(__name__)


class S3Client:
    """Client for S3 operations."""

    def __init__(self, region_name: Optional[str] = None, profile_name: Optional[str] = None):
        """
        Initialize the S3 client.

        Args:
            region_name: AWS region name
            profile_name: AWS profile name
        """
        self.region_name = region_name or config.get("aws", "region", "us-east-1")
        self.profile_name = profile_name or config.get("aws", "profile", "default")
        
        # Create session and client
        self.session = boto3.Session(
            region_name=self.region_name,
            profile_name=self.profile_name
        )
        self.client = self.session.client("s3")
        self.resource = self.session.resource("s3")

    @handle_exceptions(
        error_types={ClientError: "S3 client error"},
        default_message="Error in S3 operation"
    )
    def list_buckets(self) -> List[str]:
        """
        List all S3 buckets.

        Returns:
            List of bucket names
        """
        response = self.client.list_buckets()
        return [bucket["Name"] for bucket in response["Buckets"]]

    @handle_exceptions(
        error_types={ClientError: "S3 client error"},
        default_message="Error listing objects in S3"
    )
    def list_objects(
        self, bucket: str, prefix: str = "", delimiter: str = "/"
    ) -> Dict[str, List[str]]:
        """
        List objects in an S3 bucket.

        Args:
            bucket: S3 bucket name
            prefix: S3 prefix
            delimiter: Delimiter for hierarchical listing

        Returns:
            Dictionary with 'files' and 'prefixes' keys
        """
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter=delimiter
        )
        
        result = {"files": [], "prefixes": []}
        
        for page in pages:
            # Get common prefixes (folders)
            if "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    result["prefixes"].append(common_prefix["Prefix"])
            
            # Get objects (files)
            if "Contents" in page:
                for obj in page["Contents"]:
                    if not obj["Key"].endswith(delimiter):  # Skip folders
                        result["files"].append(obj["Key"])
        
        return result

    @retry(max_attempts=3, exceptions=(ClientError,))
    @handle_exceptions(
        error_types={ClientError: "S3 upload error"},
        default_message="Error uploading file to S3"
    )
    def upload_file(
        self, 
        local_path: str, 
        bucket: str, 
        s3_key: str, 
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Upload a file to S3.

        Args:
            local_path: Local file path
            bucket: S3 bucket name
            s3_key: S3 object key
            metadata: Optional metadata to attach to the object

        Returns:
            True if successful
        """
        extra_args = {}
        if metadata:
            extra_args["Metadata"] = metadata
        
        self.client.upload_file(
            Filename=local_path,
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs=extra_args
        )
        
        logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
        return True

    @retry(max_attempts=3, exceptions=(ClientError,))
    @handle_exceptions(
        error_types={ClientError: "S3 download error"},
        default_message="Error downloading file from S3"
    )
    def download_file(self, bucket: str, s3_key: str, local_path: str) -> bool:
        """
        Download a file from S3.

        Args:
            bucket: S3 bucket name
            s3_key: S3 object key
            local_path: Local file path

        Returns:
            True if successful
        """
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        self.client.download_file(
            Bucket=bucket,
            Key=s3_key,
            Filename=local_path
        )
        
        logger.info(f"Downloaded s3://{bucket}/{s3_key} to {local_path}")
        return True

    @handle_exceptions(
        error_types={ClientError: "S3 object deletion error"},
        default_message="Error deleting object from S3"
    )
    def delete_object(self, bucket: str, s3_key: str) -> bool:
        """
        Delete an object from S3.

        Args:
            bucket: S3 bucket name
            s3_key: S3 object key

        Returns:
            True if successful
        """
        self.client.delete_object(
            Bucket=bucket,
            Key=s3_key
        )
        
        logger.info(f"Deleted s3://{bucket}/{s3_key}")
        return True

    @handle_exceptions(
        error_types={ClientError: "S3 object copy error"},
        default_message="Error copying object in S3"
    )
    def copy_object(
        self, 
        source_bucket: str, 
        source_key: str, 
        dest_bucket: str, 
        dest_key: str
    ) -> bool:
        """
        Copy an object within S3.

        Args:
            source_bucket: Source S3 bucket name
            source_key: Source S3 object key
            dest_bucket: Destination S3 bucket name
            dest_key: Destination S3 object key

        Returns:
            True if successful
        """
        copy_source = {
            "Bucket": source_bucket,
            "Key": source_key
        }
        
        self.client.copy_object(
            CopySource=copy_source,
            Bucket=dest_bucket,
            Key=dest_key
        )
        
        logger.info(
            f"Copied s3://{source_bucket}/{source_key} to "
            f"s3://{dest_bucket}/{dest_key}"
        )
        return True

    @handle_exceptions(
        error_types={
            ClientError: "S3 object metadata error",
            KeyError: "Metadata key not found"
        },
        default_message="Error getting object metadata from S3"
    )
    def get_object_metadata(self, bucket: str, s3_key: str) -> Dict[str, Any]:
        """
        Get metadata for an S3 object.

        Args:
            bucket: S3 bucket name
            s3_key: S3 object key

        Returns:
            Dictionary of metadata
        """
        response = self.client.head_object(
            Bucket=bucket,
            Key=s3_key
        )
        
        # Extract metadata
        metadata = {
            "ContentLength": response.get("ContentLength", 0),
            "ContentType": response.get("ContentType", ""),
            "LastModified": response.get("LastModified", ""),
            "ETag": response.get("ETag", "").strip('"'),
        }
        
        # Add custom metadata
        if "Metadata" in response:
            metadata["UserMetadata"] = response["Metadata"]
        
        return metadata

    def get_s3_path(self, bucket: str, key: str) -> str:
        """
        Get the S3 path in the format s3://bucket/key.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            S3 path
        """
        return f"s3://{bucket}/{key}"

    def parse_s3_path(self, s3_path: str) -> Dict[str, str]:
        """
        Parse an S3 path into bucket and key.

        Args:
            s3_path: S3 path in the format s3://bucket/key

        Returns:
            Dictionary with 'bucket' and 'key' keys
        """
        if not s3_path.startswith("s3://"):
            raise S3Error(f"Invalid S3 path: {s3_path}")
        
        # Remove s3:// prefix
        path = s3_path[5:]
        
        # Split into bucket and key
        parts = path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        
        return {"bucket": bucket, "key": key}
