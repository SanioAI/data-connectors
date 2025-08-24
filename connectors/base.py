from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional
import logging

logger = logging.getLogger(__name__)


class DataConnector(ABC):
    """Base class for all data connectors"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validate_config(config)
        self.initialize()

    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> None:
        """Validate connector configuration"""
        pass

    @abstractmethod
    def initialize(self) -> None:
        """Initialize connector resources"""
        pass

    @abstractmethod
    def execute(self,
                input_data: Optional[Any] = None) -> Iterator[Dict[str, Any]]:
        """Execute connector operation"""
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Cleanup connector resources"""
        pass
