from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


class PipelineConfig(BaseModel):
    """Pydantic model for pipeline configuration data."""

    pipeline_id: str = Field(..., description="Unique identifier for the pipeline")
    pipeline_name: str = Field(..., description="Human-readable name of the pipeline")
    description: Optional[str] = Field(None, description="Description of the pipeline")

    # Configuration settings
    config: Dict[str, Any] = Field(default_factory=dict, description="Pipeline configuration parameters")

    # Source and destination information
    source_type: str = Field(..., description="Type of data source (e.g., 's3', 'database', 'api')")
    source_config: Dict[str, Any] = Field(default_factory=dict, description="Source configuration parameters")

    destination_type: str = Field(..., description="Type of destination (e.g., 's3', 'database', 'warehouse')")
    destination_config: Dict[str, Any] = Field(default_factory=dict, description="Destination configuration parameters")

    # Scheduling and execution
    schedule_expression: Optional[str] = Field(None, description="Cron expression for scheduling")
    is_active: bool = Field(True, description="Whether the pipeline is active")

    # Environment and deployment
    environment: str = Field(default="dev", description="Deployment environment (dev, staging, prod)")
    version: str = Field(default="1.0.0", description="Pipeline version")

    # Spark specific configurations
    spark_config: Dict[str, Any] = Field(default_factory=dict, description="Spark-specific configuration")

    # Resource requirements
    cpu_cores: Optional[int] = Field(None, description="Number of CPU cores required")
    memory_gb: Optional[Decimal] = Field(None, description="Memory requirement in GB")

    # Metadata
    tags: Dict[str, str] = Field(default_factory=dict, description="Key-value tags for the pipeline")

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Creation timestamp")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Last update timestamp")
    created_by: Optional[str] = Field(None, description="User who created the pipeline")
    updated_by: Optional[str] = Field(None, description="User who last updated the pipeline")

    model_config = ConfigDict(
        validate_assignment=True
    )

    @field_serializer('created_at', 'updated_at')
    def serialize_datetime(self, value: datetime) -> str:
        """Serialize datetime fields to ISO format."""
        return value.isoformat() if value else None

    @field_validator('created_at', 'updated_at', mode='before')
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
