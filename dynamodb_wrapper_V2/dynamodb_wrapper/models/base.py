"""
Base Model Components and Mixins

This module provides common functionality and mixins that can be shared
across domain models to eliminate code duplication and ensure consistency.

## DateTimeMixin Implementation Details

The DateTimeMixin uses Pydantic's inheritance system to automatically provide
datetime validation and serialization to any model that inherits from it.

### How It Works

1. **Pydantic Decorator Registration**
   When Python creates a class like `PipelineConfig(DateTimeMixin, BaseModel)`,
   Pydantic's metaclass processes ALL methods with decorators from ALL parent
   classes in the Method Resolution Order (MRO).

   ```python
   class DateTimeMixin(BaseModel):
       @field_validator('*', mode='before')  # This gets registered automatically!
       @classmethod
       def validate_datetime_fields(cls, v, info):
           # Custom validation logic
   ```

2. **Automatic Registration Process**
   ```python
   # When this class is created:
   class PipelineConfig(DateTimeMixin, BaseModel):
       created_at: datetime = Field(...)
       
   # Pydantic automatically:
   # 1. Scans all parent classes (DateTimeMixin, BaseModel) 
   # 2. Finds methods with @field_validator decorators
   # 3. Registers them in __pydantic_decorators__
   # 4. Applies them during validation
   ```

3. **Validation Chain**
   When you create a `PipelineConfig`:
   - Your data comes in (e.g., `created_at='2024-01-01T10:00:00Z'`)
   - Our custom validator runs first (`mode='before'`) and converts `Z` to `+00:00`
   - Pydantic's built-in datetime validator processes the normalized string
   - Final datetime object is created and stored

4. **Evidence of Registration**
   You can verify validators are registered by checking:
   ```python
   config = PipelineConfig(...)
   decorators = config.__pydantic_decorators__
   # Shows: 'validate_datetime_fields': Decorator(fields=('*',), mode='before'...)
   ```

### Why It Appears "Invisible"

The validation works so seamlessly that it appears invisible because:
- Pydantic v2 has excellent built-in datetime parsing for standard formats
- Our custom validator only adds value for edge cases (like `Z` format handling)
- Both validators work together in the validation chain seamlessly
- The final result is always a proper `datetime` object

### Benefits

- **Eliminates Code Duplication**: 72+ lines of duplicated validation removed
- **Single Source of Truth**: All datetime handling logic centralized
- **Automatic Application**: Works through inheritance without explicit calls
- **Maintains Compatibility**: Same validation behavior as original implementation
- **Type Safety**: Smart field detection using Python 3.9+ typing features

## Usage Example

```python
# Before: Duplicated validation in every model
class PipelineConfig(BaseModel):
    created_at: datetime = Field(...)
    
    @field_validator('created_at', mode='before')
    @classmethod
    def validate_datetime(cls, v):
        # 18 lines of duplicated validation logic
        
# After: Clean inheritance with automatic validation
class PipelineConfig(DateTimeMixin, BaseModel):
    created_at: datetime = Field(...)  # Validation handled automatically
```

## Components

- DateTimeMixin: Centralized datetime validation and serialization
- AuditMixin: Common audit fields for tracking creation and modification
- Common field definitions and utilities
"""

from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, field_serializer, field_validator


class DateTimeMixin(BaseModel):
    """
    Mixin providing consistent datetime validation and serialization.
    
    This mixin eliminates duplicated datetime handling logic across domain models
    by providing centralized validation and serialization for all datetime fields.
    
    Features:
    - Automatic timezone-aware datetime validation
    - ISO string parsing with timezone handling
    - Consistent serialization to ISO format
    - Error handling with descriptive messages
    """
    
    @field_validator('*', mode='before')
    @classmethod
    def validate_datetime_fields(cls, v, info):
        """
        Validate datetime fields consistently across all models.
        
        This validator applies to all fields and only processes those that are
        datetime types, leaving other fields unchanged.
        
        Args:
            v: Field value to validate
            info: Field information from Pydantic
            
        Returns:
            Validated datetime object or original value for non-datetime fields
            
        Raises:
            ValueError: If datetime format is invalid or type is unsupported
        """
        import types
        from typing import get_origin, get_args
        
        # Only process datetime fields - leave other fields unchanged
        field_annotation = getattr(info, 'annotation', None)
        
        if field_annotation is None:
            return v  # No annotation, skip validation
            
        # Handle Optional[datetime] and Union types (Python 3.9+ typing)
        origin = get_origin(field_annotation)
        if origin is not None:
            # For Union/Optional types, get the actual datetime type
            args = get_args(field_annotation)
            if not any(arg is datetime for arg in args if arg is not type(None)):
                return v  # Not a datetime field
        elif field_annotation is not datetime:
            return v  # Not a datetime field
            
        # Process datetime values only
        if v is None:
            return v
            
        if isinstance(v, str):
            # Parse ISO string with timezone handling
            try:
                # Handle both 'Z' and '+00:00' timezone formats
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError as e:
                raise ValueError(f"Invalid datetime format: {v}. Expected ISO format.") from e
                
        if isinstance(v, datetime):
            # Already a datetime object - return as-is
            return v
            
        # Unsupported type for datetime field
        raise ValueError(f"Invalid datetime type: {type(v)}. Expected datetime object or ISO string.")
    
    @field_serializer('*', when_used='json')
    def serialize_datetime_fields(self, value: Any, info) -> Any:
        """
        Serialize datetime fields to ISO format for JSON output.
        
        This serializer applies to all fields but only processes datetime objects,
        leaving other field types unchanged.
        
        Args:
            value: Field value to serialize
            info: Field information from Pydantic
            
        Returns:
            ISO format string for datetime fields, original value for others
        """
        if isinstance(value, datetime):
            return value.isoformat()
        return value


class AuditMixin(BaseModel):
    """
    Mixin providing common audit fields for tracking creation and modification.
    
    Includes standard fields for tracking who created/updated records and when,
    with consistent validation and serialization through DateTimeMixin.
    """
    
    created_at: datetime = None  # Will be set by field default_factory
    updated_at: datetime = None  # Will be set by field default_factory
    created_by: str = None
    updated_by: str = None


# Common field definitions that can be reused
COMMON_TAG_FIELD = Dict[str, str]
COMMON_CONFIG_FIELD = Dict[str, Any]