from typing import Any, Dict, Optional

from .base import DynamoDBWrapperError


class ConnectionError(DynamoDBWrapperError):
    """Raised when connection to DynamoDB fails."""

    def __init__(self, message: str, original_error: Optional[Exception] = None, context: Optional[Dict[str, Any]] = None):
        """Initialize connection error.

        Args:
            message: Human-readable error message
            original_error: The original exception that caused this error
            context: Additional context information (e.g., endpoint, region)
        """
        super().__init__(message, original_error, context)
