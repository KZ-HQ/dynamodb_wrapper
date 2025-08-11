from typing import Any, Dict, Optional


class DynamoDBWrapperError(Exception):
    """Base exception for all DynamoDB wrapper errors.

    Attributes:
        message: Human-readable error message
        original_error: The original exception that caused this error (if any)
        context: Additional context information about the error
    """

    def __init__(self, message: str, original_error: Optional[Exception] = None, context: Optional[Dict[str, Any]] = None):
        """Initialize the exception.

        Args:
            message: Human-readable error message
            original_error: The original exception that caused this error
            context: Additional context information about the error
        """
        self.message = message
        self.original_error = original_error
        self.context = context or {}
        super().__init__(message)

    def __str__(self) -> str:
        """Return string representation of the error."""
        error_str = self.message
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            error_str += f" (Context: {context_str})"
        return error_str

    def __repr__(self) -> str:
        """Return detailed string representation of the error."""
        return f"{self.__class__.__name__}(message={self.message!r}, original_error={self.original_error!r}, context={self.context!r})"
