# Base exception class
from .base import DynamoDBWrapperError

# Domain-specific exceptions (consolidated from 6 separate files)
from .domain_exceptions import (
    ValidationError,
    ItemNotFoundError,
    NotFoundError,
    ConflictError,
    ConnectionError,
    RetryableError,
)

__all__ = [
    # Base exception
    "DynamoDBWrapperError",
    
    # Domain exceptions (alphabetically ordered)
    "ConflictError",
    "ConnectionError", 
    "ItemNotFoundError",
    "NotFoundError",
    "RetryableError",
    "ValidationError",
]
