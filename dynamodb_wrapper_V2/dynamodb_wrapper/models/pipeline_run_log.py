from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


class RunStatus(str, Enum):
    """Pipeline run statuses."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class LogLevel(str, Enum):
    """Log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class StageInfo(BaseModel):
    """Information about a pipeline stage."""
    stage_name: str = Field(..., description="Name of the stage")
    status: RunStatus = Field(..., description="Status of the stage")
    start_time: Optional[datetime] = Field(None, description="Stage start time")
    end_time: Optional[datetime] = Field(None, description="Stage end time")
    duration_seconds: Optional[Decimal] = Field(None, description="Stage duration in seconds")
    records_processed: Optional[int] = Field(None, description="Number of records processed")
    error_message: Optional[str] = Field(None, description="Error message if stage failed")

    @field_serializer('start_time', 'end_time')
    def serialize_datetime(self, value: datetime) -> str:
        """Serialize datetime fields to ISO format."""
        return value.isoformat() if value else None

    @field_validator('start_time', 'end_time', mode='before')
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


class DataQualityResult(BaseModel):
    """Data quality check result."""
    check_name: str = Field(..., description="Name of the quality check")
    passed: bool = Field(..., description="Whether the check passed")
    expected_value: Optional[Any] = Field(None, description="Expected value")
    actual_value: Optional[Any] = Field(None, description="Actual value")
    error_message: Optional[str] = Field(None, description="Error message if check failed")


class PipelineRunLog(BaseModel):
    """Pydantic model for pipeline run log data."""

    run_id: str = Field(..., description="Unique identifier for this pipeline run")
    pipeline_id: str = Field(..., description="Pipeline identifier")

    # Run information
    status: RunStatus = Field(..., description="Current status of the run")
    trigger_type: str = Field(..., description="What triggered the run (schedule, manual, event)")

    # Timing information
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Run start time")
    end_time: Optional[datetime] = Field(None, description="Run end time")
    duration_seconds: Optional[Decimal] = Field(None, description="Total run duration in seconds")

    # Stage information
    stages: List[StageInfo] = Field(default_factory=list, description="Information about each stage")
    current_stage: Optional[str] = Field(None, description="Currently executing stage")

    # Data processing metrics
    total_records_processed: Optional[int] = Field(None, description="Total records processed")
    total_records_failed: Optional[int] = Field(None, description="Total records that failed processing")
    input_data_size_bytes: Optional[int] = Field(None, description="Size of input data in bytes")
    output_data_size_bytes: Optional[int] = Field(None, description="Size of output data in bytes")

    # Resource usage
    spark_application_id: Optional[str] = Field(None, description="Spark application ID")
    cpu_hours: Optional[Decimal] = Field(None, description="CPU hours consumed")
    memory_usage_gb: Optional[Decimal] = Field(None, description="Peak memory usage in GB")

    # Data quality
    data_quality_results: List[DataQualityResult] = Field(default_factory=list, description="Data quality check results")
    data_quality_passed: bool = Field(True, description="Whether all data quality checks passed")

    # Error handling
    error_message: Optional[str] = Field(None, description="Error message if run failed")
    error_stack_trace: Optional[str] = Field(None, description="Full error stack trace")
    retry_count: int = Field(0, description="Number of retries attempted")

    # Configuration used for this run
    pipeline_version: Optional[str] = Field(None, description="Version of pipeline configuration used")
    config_snapshot: Dict[str, Any] = Field(default_factory=dict, description="Snapshot of configuration used")

    # Environment information
    environment: str = Field(default="dev", description="Environment where run executed")
    spark_version: Optional[str] = Field(None, description="Spark version used")
    python_version: Optional[str] = Field(None, description="Python version used")

    # Lineage and dependencies
    input_tables: List[str] = Field(default_factory=list, description="List of input table IDs")
    output_tables: List[str] = Field(default_factory=list, description="List of output table IDs")

    # Logging and monitoring
    log_level: LogLevel = Field(LogLevel.INFO, description="Log level for this run")
    log_messages: List[Dict[str, Any]] = Field(default_factory=list, description="Structured log messages")
    monitoring_urls: Dict[str, str] = Field(default_factory=dict, description="URLs to monitoring dashboards")

    # Metadata
    tags: Dict[str, str] = Field(default_factory=dict, description="Key-value tags")
    created_by: Optional[str] = Field(None, description="User who triggered the run")

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Log creation timestamp")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Last update timestamp")

    model_config = ConfigDict(
        validate_assignment=True
    )

    @field_serializer('start_time', 'end_time', 'created_at', 'updated_at')
    def serialize_datetime(self, value: datetime) -> str:
        """Serialize datetime fields to ISO format."""
        return value.isoformat() if value else None

    @field_validator('start_time', 'end_time', 'created_at', 'updated_at', mode='before')
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
