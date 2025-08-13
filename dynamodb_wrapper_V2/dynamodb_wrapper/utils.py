"""
DynamoDB Wrapper Utilities - Consolidated Module

This single module contains all utility functions needed by the DynamoDB wrapper,
eliminating the complexity of multiple files and providing a clean, unified interface.

Key Features:
- Data serialization/deserialization (UTC-only for gateway layer)
- Query building (projections, filters, key conditions)
- Timezone management (conversion utilities)
- Model-agnostic key building using Pydantic introspection

Architecture Compliance:
- Gateway layer: UTC-only operations, no timezone conversion
- Handler layer: Timezone conversion at boundaries
- Generic model support: Works with any Pydantic BaseModel
"""

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type, Union

from pydantic import BaseModel

from .exceptions import ValidationError

logger = logging.getLogger(__name__)


# =============================================================================
# Timezone Utilities (Simplified - No Global State)
# =============================================================================

# Python 3.9+ built-in timezone support
from zoneinfo import ZoneInfo


def to_utc(dt: datetime) -> datetime:
    """Convert datetime to UTC.
    
    Handles both timezone-aware and naive datetimes. Naive datetimes are assumed
    to already be in UTC (per DynamoDB wrapper architectural contract).
    
    Args:
        dt: Datetime to convert to UTC
        
    Returns:
        Datetime in UTC timezone, or None if input is None
        
    Examples:
        >>> # Timezone-aware datetime
        >>> dt = datetime(2024, 1, 1, 10, 0, tzinfo=ZoneInfo('America/New_York'))
        >>> to_utc(dt)  # -> 2024-01-01 15:00:00+00:00
        
        >>> # Naive datetime (assumed UTC)
        >>> dt = datetime(2024, 1, 1, 10, 0)
        >>> to_utc(dt)  # -> 2024-01-01 10:00:00+00:00
    """
    if dt is None:
        return None
        
    if dt.tzinfo is None:
        # Naive datetime - assume UTC per architectural contract
        return dt.replace(tzinfo=timezone.utc)
        
    # Timezone-aware datetime - convert to UTC
    return dt.astimezone(timezone.utc)


def ensure_timezone_aware(dt: datetime, assumed_tz: Optional[str] = "UTC") -> datetime:
    """Ensure datetime has timezone information.
    
    If the datetime is naive, adds the specified timezone. If already timezone-aware,
    returns unchanged.
    
    Args:
        dt: Datetime to make timezone-aware
        assumed_tz: Timezone to assume for naive datetimes (default: "UTC")
        
    Returns:
        Timezone-aware datetime, or None if input is None
        
    Examples:
        >>> # Naive datetime - add UTC timezone
        >>> dt = datetime(2024, 1, 1, 10, 0)
        >>> ensure_timezone_aware(dt)  # -> 2024-01-01 10:00:00+00:00
        
        >>> # Naive datetime - add specific timezone
        >>> ensure_timezone_aware(dt, "America/New_York")  # -> 2024-01-01 10:00:00-05:00
    """
    if dt is None:
        return None
        
    if dt.tzinfo is not None:
        return dt  # Already timezone-aware
        
    # Add timezone to naive datetime
    if assumed_tz == "UTC":
        return dt.replace(tzinfo=timezone.utc)
    else:
        return dt.replace(tzinfo=ZoneInfo(assumed_tz))


def to_user_timezone(dt: datetime, user_tz: Optional[str] = None) -> datetime:
    """Convert UTC datetime to user's timezone for display.
    
    Args:
        dt: UTC datetime to convert
        user_tz: Target timezone for display (if None, returns unchanged)
        
    Returns:
        Datetime in user's timezone, or original datetime if user_tz is None
        
    Example:
        >>> utc_dt = datetime(2024, 1, 1, 15, 0, tzinfo=timezone.utc)
        >>> to_user_timezone(utc_dt, "America/New_York")  # -> 2024-01-01 10:00:00-05:00
    """
    if dt is None or user_tz is None:
        return dt
        
    # Convert to user timezone
    user_zone = ZoneInfo(user_tz)
    return dt.astimezone(user_zone)


# =============================================================================
# Data Serialization (Gateway Layer - UTC Only)
# =============================================================================

def item_to_model(item: Dict[str, Any], model_class: Type[BaseModel]) -> BaseModel:
    """Convert DynamoDB item to Pydantic model (UTC-only gateway utility).
    
    This utility only handles UTC datetime deserialization from DynamoDB items.
    Timezone conversion should be handled at the handler layer, not in gateway utilities.
    
    Args:
        item: DynamoDB item dictionary
        model_class: Target Pydantic model class
        
    Returns:
        Pydantic model instance with UTC datetimes
        
    Raises:
        ValidationError: If conversion fails
    """
    try:
        # Convert ISO string datetime back to UTC datetime objects and string booleans back to booleans
        def convert_datetime_strings(obj):
            if isinstance(obj, dict):
                return {k: convert_datetime_strings(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime_strings(item) for item in obj]
            elif isinstance(obj, str):
                # Try to parse as datetime first (assume UTC from DynamoDB)
                if 'T' in obj and obj.count('-') >= 2:
                    try:
                        dt = datetime.fromisoformat(obj.replace('Z', '+00:00'))
                        return dt
                    except ValueError:
                        pass
                # Try to parse as boolean
                if obj.lower() in ('true', 'false'):
                    return obj.lower() == 'true'
                return obj
            else:
                return obj

        converted_item = convert_datetime_strings(item)
        return model_class(**converted_item)
    except Exception as e:
        logger.error(f"Failed to convert item to model: {e}")
        raise ValidationError(f"Failed to convert item to model: {e}") from e


def model_to_item(model: BaseModel) -> Dict[str, Any]:
    """Convert Pydantic model to DynamoDB item (UTC-only gateway utility).
    
    This utility only handles UTC datetime serialization for DynamoDB storage.
    All datetime objects should already be in UTC when reaching the gateway layer.
    
    Args:
        model: Pydantic model instance with UTC datetimes
        
    Returns:
        DynamoDB item dictionary with ISO datetime strings
    """
    item = model.model_dump(exclude_none=True)

    # Convert datetime objects to ISO strings and booleans to strings for DynamoDB storage
    def convert_datetime(obj):
        if isinstance(obj, dict):
            return {k: convert_datetime(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_datetime(item) for item in obj]
        elif isinstance(obj, datetime):
            # Store as ISO string (datetime should already be UTC)
            return obj.isoformat()
        elif isinstance(obj, bool):
            # Convert booleans to strings for DynamoDB GSI compatibility
            return str(obj).lower()
        else:
            return obj

    return convert_datetime(item)


# =============================================================================
# Query Building Utilities
# =============================================================================

def build_projection_expression(fields: Optional[List[str]]) -> tuple[Optional[str], Optional[Dict[str, str]]]:
    """Build ProjectionExpression with ExpressionAttributeNames for DynamoDB operations.
    
    This helper safely handles DynamoDB reserved words by using expression attribute names.
    Projections reduce RCU consumption and latency by limiting returned data.
    
    Args:
        fields: List of field names to project, None for all fields
        
    Returns:
        Tuple of (ProjectionExpression, ExpressionAttributeNames) or (None, None)
        
    Example:
        >>> build_projection_expression(['pipeline_id', 'status', 'data'])
        ('#f0, #f1, #f2', {'#f0': 'pipeline_id', '#f1': 'status', '#f2': 'data'})
        
    Note:
        - Reduces RCU consumption (fewer bytes transferred)
        - Reduces latency (smaller payloads)
        - Handles DynamoDB reserved words safely
    """
    if not fields:
        return None, None
    
    # Build expression attribute names to handle reserved words safely
    expression_names = {}
    projection_parts = []
    
    for i, field in enumerate(fields):
        attr_name = f"#f{i}"
        expression_names[attr_name] = field
        projection_parts.append(attr_name)
    
    projection_expression = ', '.join(projection_parts)
    return projection_expression, expression_names


def build_filter_expression(filters: Dict[str, Any]):
    """Build FilterExpression for DynamoDB operations.
    
    Args:
        filters: Dictionary of attribute names to values
        
    Returns:
        FilterExpression for boto3, or None if no filters
        
    Example:
        >>> build_filter_expression({'is_active': True, 'environment': 'prod'})
        # Returns: Attr('is_active').eq(True) & Attr('environment').eq('prod')
    """
    from boto3.dynamodb.conditions import Attr
    
    if not filters:
        return None
        
    conditions = []
    for attr_name, value in filters.items():
        conditions.append(Attr(attr_name).eq(value))
    
    # Combine conditions with AND
    filter_expr = conditions[0]
    for condition in conditions[1:]:
        filter_expr = filter_expr & condition
        
    return filter_expr


def build_key_condition(
    partition_key: str,
    partition_value: Any,
    sort_key: Optional[str] = None,
    sort_condition: str = "eq",
    sort_value: Optional[Any] = None,
    sort_value2: Optional[Any] = None
):
    """Build KeyConditionExpression for DynamoDB queries with comprehensive condition support.
    
    This unified implementation supports all query patterns needed by the CQRS architecture.
    
    Args:
        partition_key: Partition key attribute name
        partition_value: Partition key value
        sort_key: Sort key attribute name (optional)
        sort_condition: Sort key condition type ('eq', 'begins_with', 'between', 'gt', 'gte', 'lt', 'lte')
        sort_value: Sort key value
        sort_value2: Second sort key value (required for 'between' condition)
        
    Returns:
        KeyConditionExpression for boto3
        
    Examples:
        Simple partition key only:
        >>> build_key_condition('pipeline_id', 'pipeline-123')
        
        Equal condition with sort key:
        >>> build_key_condition('pipeline_id', 'pipeline-1', 'run_id', 'eq', 'run-123')
        
        Range condition:
        >>> build_key_condition('pipeline_id', 'pipeline-1', 'created_at', 'between', '2024-01-01', '2024-01-31')
        
        Prefix matching:
        >>> build_key_condition('pipeline_id', 'pipeline-1', 'run_id', 'begins_with', 'run-2024')
    
    Raises:
        ValueError: For invalid sort_condition or missing sort_value2 for 'between'
    """
    from boto3.dynamodb.conditions import Key
    
    # Build partition key condition (always required)
    condition = Key(partition_key).eq(partition_value)
    
    # Add sort key condition if provided
    if sort_key and sort_value is not None:
        sort_key_obj = Key(sort_key)
        
        # Apply the appropriate condition based on sort_condition
        if sort_condition == "eq":
            condition = condition & sort_key_obj.eq(sort_value)
        elif sort_condition == "begins_with":
            condition = condition & sort_key_obj.begins_with(sort_value)
        elif sort_condition == "between":
            if sort_value2 is None:
                raise ValueError("'between' condition requires sort_value2 parameter")
            condition = condition & sort_key_obj.between(sort_value, sort_value2)
        elif sort_condition == "gt":
            condition = condition & sort_key_obj.gt(sort_value)
        elif sort_condition == "gte":
            condition = condition & sort_key_obj.gte(sort_value)
        elif sort_condition == "lt":
            condition = condition & sort_key_obj.lt(sort_value)
        elif sort_condition == "lte":
            condition = condition & sort_key_obj.lte(sort_value)
        else:
            raise ValueError(
                f"Unsupported sort_condition: {sort_condition}. "
                f"Supported values: eq, begins_with, between, gt, gte, lt, lte"
            )
    
    return condition


# =============================================================================
# Domain Model Introspection (Meta Class Only)
# =============================================================================

def extract_model_metadata(model_class: Type[BaseModel]) -> Dict[str, Any]:
    """Extract metadata from a Pydantic model's Meta class.
    
    This function requires domain models to have a proper Meta class definition.
    No fallback strategies - Meta class is the single source of truth.
    
    Args:
        model_class: Pydantic BaseModel class with Meta class
        
    Returns:
        Dictionary with model metadata
        
    Raises:
        ValueError: If model doesn't have required Meta class attributes
        
    Example:
        >>> metadata = extract_model_metadata(PipelineConfig)
        >>> metadata['partition_key']
        'pipeline_id'
    """
    if not hasattr(model_class, 'Meta'):
        raise ValueError(f"Model {model_class.__name__} must have a Meta class with partition_key, sort_key, and gsis attributes")
    
    meta = model_class.Meta
    
    # Extract required metadata from Meta class
    partition_key = getattr(meta, 'partition_key', None)
    sort_key = getattr(meta, 'sort_key', None)  # Can be None for simple keys
    gsis = getattr(meta, 'gsis', [])
    
    if not partition_key:
        raise ValueError(f"Model {model_class.__name__}.Meta must define partition_key")
    
    return {
        'partition_key': partition_key,
        'sort_key': sort_key,
        'primary_key_fields': [k for k in [partition_key, sort_key] if k],
        'available_fields': list(model_class.model_fields.keys()),
        'gsis': gsis
    }


# =============================================================================
# Domain Model Key Building (Meta Class Based)
# =============================================================================

def build_model_key(model_class: Type[BaseModel], **key_values: Any) -> Dict[str, Any]:
    """Build a DynamoDB key using domain model Meta class definitions.
    
    This function requires domain models to have proper Meta class with
    partition_key, sort_key, and gsis attributes defined.
    
    Args:
        model_class: Pydantic BaseModel class with Meta class
        **key_values: Key-value pairs for the key fields
        
    Returns:
        DynamoDB key dictionary
        
    Examples:
        >>> build_model_key(PipelineConfig, pipeline_id="pipeline-123")
        {'pipeline_id': 'pipeline-123'}
        
        >>> build_model_key(PipelineRunLog, run_id="run-456", pipeline_id="pipeline-123")
        {'run_id': 'run-456', 'pipeline_id': 'pipeline-123'}
        
    Raises:
        ValueError: If model lacks Meta class, or required key fields are missing
    """
    metadata = extract_model_metadata(model_class)
    key = {}
    
    # Add partition key
    partition_key = metadata['partition_key']
    if not partition_key:
        raise ValueError(f"Could not determine partition key for {model_class.__name__}")
        
    if partition_key in key_values:
        key[partition_key] = key_values[partition_key]
    else:
        raise ValueError(f"Missing partition key '{partition_key}' for {model_class.__name__}")
    
    # Add sort key if defined
    sort_key = metadata['sort_key']
    if sort_key:
        if sort_key in key_values:
            key[sort_key] = key_values[sort_key]
        else:
            raise ValueError(f"Missing sort key '{sort_key}' for {model_class.__name__}")
    
    return key


def build_model_key_condition(
    model_class: Type[BaseModel],
    sort_condition: str = "eq",
    sort_value2: Optional[Any] = None,
    **key_values: Any
):
    """Build KeyConditionExpression using domain model Meta class definitions.
    
    Args:
        model_class: Pydantic BaseModel class with Meta class
        sort_condition: Sort key condition type ('eq', 'begins_with', 'between', 'gt', 'gte', 'lt', 'lte')
        sort_value2: Second sort key value (for 'between' condition)
        **key_values: Key-value pairs for the key fields
        
    Returns:
        KeyConditionExpression for boto3
        
    Examples:
        >>> build_model_key_condition(PipelineConfig, pipeline_id="pipeline-123")
        
        >>> build_model_key_condition(PipelineRunLog, sort_condition="between", 
        ...                           run_id="run-456", pipeline_id="pipeline-123")
        
    Raises:
        ValueError: If model lacks Meta class, required key fields missing, or invalid sort_condition
    """
    metadata = extract_model_metadata(model_class)
    
    # Extract partition key
    partition_key = metadata['partition_key']
    if not partition_key:
        raise ValueError(f"Could not determine partition key for {model_class.__name__}")
        
    if partition_key not in key_values:
        raise ValueError(f"Missing partition key '{partition_key}' for {model_class.__name__}")
    
    partition_value = key_values[partition_key]
    
    # Extract sort key if present
    sort_key = metadata['sort_key']
    sort_value = key_values.get(sort_key) if sort_key else None
    
    # Use the unified build_key_condition function
    return build_key_condition(
        partition_key=partition_key,
        partition_value=partition_value,
        sort_key=sort_key,
        sort_condition=sort_condition,
        sort_value=sort_value,
        sort_value2=sort_value2
    )


def build_gsi_key_condition(
    model_class: Type[BaseModel],
    gsi_name: str,
    sort_condition: str = "eq",
    sort_value2: Optional[Any] = None,
    **key_values: Any
):
    """Build KeyConditionExpression for GSI using model Meta class definitions.
    
    Args:
        model_class: Pydantic BaseModel class with Meta class
        gsi_name: Name of the GSI to query
        sort_condition: Sort key condition type
        sort_value2: Second sort key value (for 'between' condition)
        **key_values: Key-value pairs for the key fields
        
    Returns:
        KeyConditionExpression for boto3 GSI query
        
    Raises:
        ValueError: If model has no Meta class, GSI not found, or required keys missing
    """
    metadata = extract_model_metadata(model_class)  # This validates Meta class exists
    
    # Find the GSI definition in the Meta class
    gsi_def = None
    for gsi in metadata['gsis']:
        if gsi.name == gsi_name:
            gsi_def = gsi
            break
    
    if not gsi_def:
        available_gsis = [gsi.name for gsi in metadata['gsis']]
        raise ValueError(f"GSI '{gsi_name}' not found in {model_class.__name__}.Meta. Available GSIs: {available_gsis}")
    
    # Check for GSI partition key
    if not key_values or gsi_def.partition_key not in key_values:
        raise ValueError(f"Missing GSI partition key '{gsi_def.partition_key}' for GSI '{gsi_name}'")
    
    gsi_partition_key = gsi_def.partition_key
    gsi_partition_value = key_values[gsi_partition_key]
    
    # Check for GSI sort key
    gsi_sort_key = gsi_def.sort_key
    gsi_sort_value = key_values.get(gsi_sort_key) if gsi_sort_key else None
    
    return build_key_condition(
        partition_key=gsi_partition_key,
        partition_value=gsi_partition_value,
        sort_key=gsi_sort_key,
        sort_condition=sort_condition,
        sort_value=gsi_sort_value,
        sort_value2=sort_value2
    )


# =============================================================================
# Convenience Functions for Common Operations
# =============================================================================

def get_model_primary_key_fields(model_class: Type[BaseModel]) -> List[str]:
    """Get primary key field names from domain model Meta class.
    
    Args:
        model_class: Pydantic BaseModel class with Meta class
        
    Returns:
        List of primary key field names
        
    Examples:
        >>> get_model_primary_key_fields(PipelineConfig)
        ['pipeline_id']
        
    Raises:
        ValueError: If model lacks required Meta class
    """
    metadata = extract_model_metadata(model_class)
    return metadata['primary_key_fields']


def get_model_gsi_names(model_class: Type[BaseModel]) -> List[str]:
    """Get GSI names from model Meta class.
    
    Args:
        model_class: Pydantic BaseModel class with Meta class
        
    Returns:
        List of GSI names from model Meta class
        
    Raises:
        ValueError: If model doesn't have required Meta class
    """
    metadata = extract_model_metadata(model_class)  # This handles Meta class validation
    return [gsi.name for gsi in metadata['gsis']]


# =============================================================================
# Handler Layer Timezone Utilities (Simplified)
# =============================================================================

def ensure_utc_for_storage(dt: datetime, default_timezone: str = "UTC") -> datetime:
    """Ensure datetime is in UTC for DynamoDB storage.
    
    Converts any datetime to UTC. Naive datetimes are assumed to be in the
    specified timezone (defaults to UTC for library consistency).
    
    Args:
        dt: Datetime to convert to UTC
        default_timezone: Timezone to assume for naive datetimes (should be "UTC" for internal ops)
        
    Returns:
        UTC datetime ready for storage
        
    Note:
        For internal library operations, this should always use default_timezone="UTC"
        to maintain data consistency. The parameter exists for edge cases only.
        
    Example:
        >>> dt = datetime(2024, 1, 1, 10, 0)  # naive datetime
        >>> ensure_utc_for_storage(dt)  # -> UTC datetime (recommended)
        >>> ensure_utc_for_storage(dt, "UTC")  # -> Same as above (explicit)
    """
    if dt is None:
        return None
        
    # Make timezone-aware if needed (using UTC for consistency), then convert to UTC
    dt = ensure_timezone_aware(dt, default_timezone)
    return to_utc(dt)


# =============================================================================
# Note: DynamoDBKey class removed as it was unused in V2 codebase
# The V2 architecture uses direct function calls like build_model_key() instead
# =============================================================================


# =============================================================================
# Export All Functions
# =============================================================================

__all__ = [
    # Timezone Utilities (Simplified)
    "to_utc",
    "ensure_timezone_aware", 
    "to_user_timezone",
    "ensure_utc_for_storage",
    
    # Data Layer (Gateway)
    "item_to_model",
    "model_to_item",
    
    # Query Building
    "build_projection_expression",
    "build_filter_expression",
    "build_key_condition",
    
    # Generic Model-Aware Operations
    "extract_model_metadata",
    "build_model_key",
    "build_model_key_condition",
    "build_gsi_key_condition",
    "get_model_primary_key_fields",
    "get_model_gsi_names",
]