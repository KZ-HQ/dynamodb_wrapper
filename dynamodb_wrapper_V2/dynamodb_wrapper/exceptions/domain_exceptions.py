"""
Domain-Specific Exceptions for DynamoDB Wrapper

This module consolidates all domain-specific exceptions that extend the base
DynamoDBWrapperError. These exceptions represent business logic failures,
validation errors, and domain-specific error conditions.

Organized by category:
1. Data Validation Errors
2. Resource Not Found Errors  
3. Conflict and Conditional Errors
4. Infrastructure and Retry Errors
"""

from typing import Any, Dict, Optional

from .base import DynamoDBWrapperError


# =============================================================================
# Data Validation Errors
# =============================================================================

class ValidationError(DynamoDBWrapperError):
    """Raised when data validation fails.
    
    Used for:
    - Pydantic model validation failures
    - Business rule validation failures
    - Data format/type validation errors
    - Required field validation errors
    """

    def __init__(self, message: str, errors: Optional[Dict[str, Any]] = None, original_error: Optional[Exception] = None):
        """Initialize validation error.

        Args:
            message: Human-readable error message
            errors: Dictionary of field-level validation errors
            original_error: The original exception that caused this error
        """
        self.errors = errors or {}
        context = {
            'validation_errors': self.errors
        }
        super().__init__(message, original_error, context)


# =============================================================================
# Resource Not Found Errors
# =============================================================================

class ItemNotFoundError(DynamoDBWrapperError):
    """Raised when a specific item is not found in DynamoDB.
    
    Used for:
    - GetItem operations that return no results
    - Update/Delete operations on non-existent items
    - Conditional operations that expect existing items
    """

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


class NotFoundError(DynamoDBWrapperError):
    """Raised when a DynamoDB resource (table, index, item) is not found.
    
    Used for:
    - Table or index not found errors
    - General resource not found scenarios
    - Infrastructure-level not found errors
    """

    def __init__(self, message: str, resource_type: Optional[str] = None, resource_name: Optional[str] = None, original_error: Optional[Exception] = None):
        """Initialize not found error.

        Args:
            message: Human-readable error message
            resource_type: Type of resource not found (e.g., 'table', 'index', 'item')
            resource_name: Name of the resource not found
            original_error: The original exception that caused this error
        """
        self.resource_type = resource_type
        self.resource_name = resource_name
        context = {}
        if resource_type:
            context['resource_type'] = resource_type
        if resource_name:
            context['resource_name'] = resource_name
        super().__init__(message, original_error, context)


# =============================================================================
# Conflict and Conditional Errors
# =============================================================================

class ConflictError(DynamoDBWrapperError):
    """Raised when a conditional operation fails due to existing data.
    
    Used for:
    - ConditionalCheckFailedException from DynamoDB
    - Race conditions in concurrent updates
    - Business rule violations (e.g., duplicate pipeline IDs)
    - Optimistic locking failures
    """

    def __init__(self, message: str, resource_id: Optional[str] = None, original_error: Optional[Exception] = None):
        """Initialize conflict error.

        Args:
            message: Human-readable error message
            resource_id: ID of the conflicting resource (e.g., pipeline_id)
            original_error: The original exception that caused this error
        """
        self.resource_id = resource_id
        context = {}
        if resource_id:
            context['resource_id'] = resource_id
        super().__init__(message, original_error, context)


# =============================================================================
# Infrastructure and Retry Errors
# =============================================================================

class ConnectionError(DynamoDBWrapperError):
    """Raised when connection to DynamoDB fails.
    
    Used for:
    - Network connectivity issues
    - Authentication/authorization failures
    - DynamoDB service unavailable
    - Timeout errors
    - Invalid endpoint configurations
    """

    def __init__(self, message: str, original_error: Optional[Exception] = None, context: Optional[Dict[str, Any]] = None):
        """Initialize connection error.

        Args:
            message: Human-readable error message
            original_error: The original exception that caused this error
            context: Additional context information (e.g., endpoint, region)
        """
        super().__init__(message, original_error, context)


class RetryableError(DynamoDBWrapperError):
    """Raised when operation fails due to temporary/throttling issues that can be retried.
    
    Used for:
    - ProvisionedThroughputExceededException
    - RequestLimitExceeded errors
    - Temporary service unavailability
    - Network timeouts that should be retried
    - Transaction conflicts that can be retried
    """

    def __init__(self, message: str, retry_after_seconds: Optional[int] = None, original_error: Optional[Exception] = None):
        """Initialize retryable error.

        Args:
            message: Human-readable error message
            retry_after_seconds: Suggested retry delay in seconds
            original_error: The original exception that caused this error
        """
        self.retry_after_seconds = retry_after_seconds
        context = {}
        if retry_after_seconds:
            context['retry_after_seconds'] = retry_after_seconds
        super().__init__(message, original_error, context)