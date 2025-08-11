from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


class TableType(str, Enum):
    """Supported table types."""
    SOURCE = "source"
    DESTINATION = "destination"
    LOOKUP = "lookup"
    INTERMEDIATE = "intermediate"


class DataFormat(str, Enum):
    """Supported data formats."""
    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"
    AVRO = "avro"
    DELTA = "delta"


class TableConfig(BaseModel):
    """Pydantic model for table configuration data."""

    table_id: str = Field(..., description="Unique identifier for the table")
    pipeline_id: str = Field(..., description="Pipeline this table belongs to")
    table_name: str = Field(..., description="Physical table name")

    # Table classification
    table_type: TableType = Field(..., description="Type of table in the pipeline")
    data_format: DataFormat = Field(..., description="Data format of the table")

    # Schema information
    schema_definition: Dict[str, Any] = Field(default_factory=dict, description="Table schema definition")
    partition_columns: List[str] = Field(default_factory=list, description="Partition columns")
    primary_key_columns: List[str] = Field(default_factory=list, description="Primary key columns")

    # Location and connection
    location: str = Field(..., description="Physical location/path of the table")
    connection_config: Dict[str, Any] = Field(default_factory=dict, description="Connection configuration")

    # Processing configuration
    read_options: Dict[str, Any] = Field(default_factory=dict, description="Options for reading the table")
    write_options: Dict[str, Any] = Field(default_factory=dict, description="Options for writing to the table")

    # Data quality and validation
    validation_rules: Dict[str, Any] = Field(default_factory=dict, description="Data validation rules")
    data_quality_checks: List[Dict[str, Any]] = Field(default_factory=list, description="Data quality check configurations")

    # Performance optimization
    caching_enabled: bool = Field(False, description="Whether caching is enabled for this table")
    cache_level: Optional[str] = Field(None, description="Spark cache level (MEMORY_ONLY, MEMORY_AND_DISK, etc.)")

    # Lifecycle management
    retention_days: Optional[int] = Field(None, description="Data retention period in days")
    archive_after_days: Optional[int] = Field(None, description="Archive data after specified days")

    # Environment and deployment
    environment: str = Field(default="dev", description="Environment (dev, staging, prod)")
    is_active: bool = Field(True, description="Whether the table configuration is active")

    # Metadata
    description: Optional[str] = Field(None, description="Table description")
    tags: Dict[str, str] = Field(default_factory=dict, description="Key-value tags")

    # Statistics (updated by pipeline runs)
    last_updated_data: Optional[datetime] = Field(None, description="Last data update timestamp")
    record_count: Optional[int] = Field(None, description="Approximate record count")
    size_bytes: Optional[int] = Field(None, description="Table size in bytes")

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Creation timestamp")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Last update timestamp")
    created_by: Optional[str] = Field(None, description="User who created the configuration")
    updated_by: Optional[str] = Field(None, description="User who last updated the configuration")

    model_config = ConfigDict(
        validate_assignment=True
    )

    @field_serializer('created_at', 'updated_at', 'last_updated_data')
    def serialize_datetime(self, value: datetime) -> str:
        """Serialize datetime fields to ISO format."""
        return value.isoformat() if value else None

    @field_validator('created_at', 'updated_at', 'last_updated_data', mode='before')
    @classmethod
    def validate_datetime(cls, v):
        """Validate and ensure timezone-aware datetime."""
        if v is None:
            return v

        if isinstance(v, str):
            # Parse ISO string
            try:
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError as e:
                raise ValueError(f"Invalid datetime format: {v}") from e

        if isinstance(v, datetime):
            return v

        raise ValueError(f"Invalid datetime type: {type(v)}")
