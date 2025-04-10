"""
Data ingestion module for the Bronze layer.
"""
import os
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd

from config.config import config
from src.utils.error_handling import handle_exceptions
from src.utils.logging_utils import setup_logger
from src.utils.s3 import S3Client

# Set up logger
logger = setup_logger(__name__)


class BronzeIngestion:
    """Class for ingesting data into the Bronze layer."""

    def __init__(
        self,
        source_type: str,
        target_bucket: Optional[str] = None,
        target_prefix: Optional[str] = None,
    ):
        """
        Initialize the Bronze ingestion.

        Args:
            source_type: Type of source data (e.g., 'csv', 'json', 'parquet')
            target_bucket: Target S3 bucket for Bronze data
            target_prefix: Target S3 prefix for Bronze data
        """
        self.source_type = source_type
        self.target_bucket = target_bucket or config.get("s3", "bronze_bucket")
        self.target_prefix = target_prefix or config.get("s3", "prefix", "data")
        
        # Initialize S3 client
        self.s3_client = S3Client()
        
        logger.info(
            f"Initialized Bronze ingestion for {source_type} data to "
            f"s3://{self.target_bucket}/{self.target_prefix}"
        )

    @handle_exceptions(default_message="Error in file ingestion")
    def ingest_file(
        self,
        source_path: str,
        partition_keys: Optional[List[str]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Ingest a file into the Bronze layer.

        Args:
            source_path: Path to the source file (local or S3)
            partition_keys: List of partition keys
            metadata: Additional metadata to store with the file

        Returns:
            S3 path where the data was stored
        """
        # Generate target path with partitions if provided
        target_key = self._generate_target_key(source_path, partition_keys)
        
        # Add timestamp to metadata
        if metadata is None:
            metadata = {}
        
        metadata["ingestion_timestamp"] = datetime.now().isoformat()
        metadata["source_file"] = os.path.basename(source_path)
        metadata["source_type"] = self.source_type
        
        # Upload file to S3
        if source_path.startswith("s3://"):
            # Copy from S3 to S3
            source_parts = self.s3_client.parse_s3_path(source_path)
            self.s3_client.copy_object(
                source_bucket=source_parts["bucket"],
                source_key=source_parts["key"],
                dest_bucket=self.target_bucket,
                dest_key=target_key
            )
        else:
            # Upload from local to S3
            self.s3_client.upload_file(
                local_path=source_path,
                bucket=self.target_bucket,
                s3_key=target_key,
                metadata=metadata
            )
        
        target_s3_path = self.s3_client.get_s3_path(self.target_bucket, target_key)
        logger.info(f"Ingested {source_path} to {target_s3_path}")
        
        return target_s3_path

    @handle_exceptions(default_message="Error in dataframe ingestion")
    def ingest_dataframe(
        self,
        df: pd.DataFrame,
        name: str,
        partition_keys: Optional[List[str]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Ingest a pandas DataFrame into the Bronze layer.

        Args:
            df: Pandas DataFrame to ingest
            name: Name for the dataset
            partition_keys: List of partition keys
            metadata: Additional metadata to store with the file

        Returns:
            S3 path where the data was stored
        """
        import tempfile
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(
            suffix=f".{self.source_type}", delete=False
        ) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Write DataFrame to the temporary file
            if self.source_type == "csv":
                df.to_csv(temp_path, index=False)
            elif self.source_type == "parquet":
                df.to_parquet(temp_path, index=False)
            elif self.source_type == "json":
                df.to_json(temp_path, orient="records", lines=True)
            else:
                raise ValueError(f"Unsupported source type: {self.source_type}")
            
            # Ingest the temporary file
            return self.ingest_file(
                source_path=temp_path,
                partition_keys=partition_keys,
                metadata=metadata
            )
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _generate_target_key(
        self, source_path: str, partition_keys: Optional[List[str]] = None
    ) -> str:
        """
        Generate the target S3 key with partitions.

        Args:
            source_path: Source file path
            partition_keys: List of partition keys

        Returns:
            Target S3 key
        """
        # Get the base filename
        filename = os.path.basename(source_path)
        
        # Generate timestamp-based path
        timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
        
        # Build the target key
        if partition_keys:
            # Add partition information to the key
            partitions = "/".join(partition_keys)
            target_key = f"{self.target_prefix}/{partitions}/{timestamp}/{filename}"
        else:
            target_key = f"{self.target_prefix}/{timestamp}/{filename}"
        
        return target_key
