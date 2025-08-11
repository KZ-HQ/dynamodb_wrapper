from typing import Optional

from .base import DynamoDBWrapperError


class ItemNotFoundError(DynamoDBWrapperError):
    """Raised when an item is not found in DynamoDB."""

    def __init__(self, table_name: str, key: dict, original_error: Optional[Exception] = None):
        """Initialize item not found error.

        Args:
            table_name: Name of the DynamoDB table
            key: The key that was not found
            original_error: The original exception that caused this error
        """
        self.table_name = table_name
        self.key = key
        message = f"Item not found in table '{table_name}' with key: {key}"
        context = {
            'table_name': table_name,
            'key': key
        }
        super().__init__(message, original_error, context)
