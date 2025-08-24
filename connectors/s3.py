import boto3
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Any, Dict, Iterator, Optional
from connectors.base import DataConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("s3_connector")


class S3SinkConnector(DataConnector):
    """S3 sink connector for writing data"""

    def validate_config(self, config: Dict[str, Any]) -> None:
        required_fields = ['bucket_name', 'key_prefix']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")

    def initialize(self) -> None:
        """Initialize S3 client"""
        try:
            session = boto3.Session(
                aws_access_key_id=self.config.get('access_key_id'),
                aws_secret_access_key=self.config.get(
                    'secret_access_key'),
                region_name=self.config.get('region', 'us-east-1')
            )

            self.s3_client = session.client(
                's3',
                endpoint_url=self.config.get('endpoint_url')
            )
            self.format_handlers = {
                'parquet': self._write_parquet,
                'json': self._write_json,
                'csv': self._write_csv,
            }
            logger.info("S3 client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise

    def execute(
            self, input_data: Optional[Any] = None
    ) -> Iterator[Dict[str, Any]]:
        """Write input data to S3"""
        if not input_data:
            logger.warning("No input data provided to S3 sink")
            return

        bucket_name = self.config['bucket_name']
        key_prefix = self.config['key_prefix']
        file_format = self.config.get(
            'format', 'parquet')  # parquet, json, csv

        # Format handlers mapping
        try:
            # Generate unique file key
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_key = f"{key_prefix}/data_{timestamp}.{file_format}"

            # Get format handler
            if file_format not in self.format_handlers:
                raise ValueError(
                    f"Unsupported format: {file_format}")

            handler = self.format_handlers[file_format]
            handler(input_data['data'], bucket_name, file_key)
            records_length = len(
                input_data['data']) if 'data' in input_data else 0

            yield {
                'status': 'success',
                'file_location': f's3://{bucket_name}/{file_key}',
                'records_written': records_length,
                'metadata': input_data.get('metadata', {})
            }

        except Exception as e:
            logger.error(f"Error writing to S3: {e}")
            yield {
                'status': 'error',
                'error': str(e)
            }

    def _write_parquet(self, data: list, bucket: str, key: str) -> None:
        """Write data as Parquet format"""
        df = pd.DataFrame(data)
        parquet_buffer = df.to_parquet(index=False)
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=parquet_buffer)

    def _write_json(self, data: list, bucket: str, key: str) -> None:
        """Write data as JSON format"""
        json_data = json.dumps(data, indent=2, default=str)
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=json_data)

    def _write_csv(self, data: list, bucket: str, key: str) -> None:
        """Write data as CSV format"""
        df = pd.DataFrame(data)
        csv_data = df.to_csv(index=False)
        self.s3_client.put_object(Bucket=bucket, Key=key, Body=csv_data)

    def cleanup(self) -> None:
        """Cleanup S3 resources"""
        # S3 client doesn't require explicit cleanup
        logger.info("S3 connector cleanup completed")
