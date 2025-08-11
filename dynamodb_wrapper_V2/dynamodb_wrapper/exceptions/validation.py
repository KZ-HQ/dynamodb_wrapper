from typing import Any, Dict, Optional

from .base import DynamoDBWrapperError


class ValidationError(DynamoDBWrapperError):
    """Raised when data validation fails."""

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
