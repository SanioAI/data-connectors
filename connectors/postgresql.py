import logging
from typing import Any, Dict, Iterator, Optional

import psycopg2

from connectors.base import DataConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("postgres_connector")


class PostgreSQLSourceConnector(DataConnector):
    """PostgreSQL source connector for reading data"""

    def validate_config(self, config: Dict[str, Any]) -> None:
        required_fields = ['host', 'database', 'username', 'password', 'query']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")

    def initialize(self) -> None:
        """Initialize PostgreSQL connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['username'],
                password=self.config['password'],
                port=self.config.get('port', 5432)
            )
            logger.info("PostgreSQL connection established")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def execute(
            self, input_data: Optional[Any] = None
    ) -> Iterator[Dict[str, Any]]:
        """Execute SQL query and yield results in batches"""
        batch_size = self.config.get('batch_size', 1000)

        try:
            cursor = self.connection.cursor()
            cursor.execute(self.config['query'])

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break

                # Convert to list of dictionaries
                batch_data = [dict(zip(columns, row)) for row in rows]

                yield {
                    'data': batch_data,
                    'metadata': {
                        'source': 'postgresql',
                        'batch_size': len(batch_data),
                        'columns': columns
                    }
                }

            cursor.close()

        except Exception as e:
            logger.error(f"Error executing PostgreSQL query: {e}")
            raise

    def cleanup(self) -> None:
        """Close PostgreSQL connection"""
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed")
