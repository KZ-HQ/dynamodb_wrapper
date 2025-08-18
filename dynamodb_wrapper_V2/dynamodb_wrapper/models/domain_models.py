"""
Domain Models for DynamoDB Wrapper

This module consolidates all core domain models that represent business entities
in the DynamoDB wrapper library. These are the foundational models used across
the entire system for data validation, serialization, and business logic.

Organized by domain:
1. Pipeline Configuration Models
2. Table Configuration Models  
3. Pipeline Run Log Models

Each domain includes:
- Main business entity model
- Supporting enums and data classes
- Shared validation logic
- Consistent datetime handling
"""

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from .base import DateTimeMixin, DynamoDBMixin


# =============================================================================
# DynamoDB Table Metadata Classes
# =============================================================================

class GSIDefinition:
    """Defines a Global Secondary Index for DynamoDB."""
    def __init__(
        self, 
        name: str, 
        partition_key: str, 
        sort_key: Optional[str] = None,
        projection: Optional[List[str]] = None
    ):
        self.name = name
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.projection = projection  # None means ALL attributes


class TableMeta:
    """Base class for table metadata definitions."""
    table_name: str
    partition_key: str
    sort_key: Optional[str] = None
    gsis: List[GSIDefinition] = []
    
    @classmethod
    def get_key_fields(cls) -> List[str]:
        """Get DynamoDB item key field names.
        
        Returns the list of fields that form the DynamoDB item key:
        - For simple keys: [partition_key]
        - For composite keys: [partition_key, sort_key]
        """
        fields = [cls.partition_key]
        if cls.sort_key:
            fields.append(cls.sort_key)
        return fields
    
    @classmethod
    def get_gsi_by_name(cls, gsi_name: str) -> Optional[GSIDefinition]:
        """Get GSI definition by name."""
        for gsi in cls.gsis:
            if gsi.name == gsi_name:
                return gsi
        return None
    
    @classmethod
    def get_gsis_by_partition_key(cls, partition_key: str) -> List[GSIDefinition]:
        """Get all GSIs that use a specific partition key."""
        return [gsi for gsi in cls.gsis if gsi.partition_key == partition_key]


# =============================================================================
# Pipeline Configuration Domain
# =============================================================================

class PipelineConfig(DynamoDBMixin, DateTimeMixin, BaseModel):
    """
    Core domain model for pipeline configuration data.
    
    Represents the complete configuration of a data processing pipeline
    including source/destination information, scheduling, resource requirements,
    and execution parameters.
    
    Uses DateTimeMixin for consistent datetime validation and serialization.
    """

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

    # Timestamps - datetime validation/serialization handled by DateTimeMixin
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Creation timestamp")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Last update timestamp")
    created_by: Optional[str] = Field(None, description="User who created the pipeline")
    updated_by: Optional[str] = Field(None, description="User who last updated the pipeline")

    model_config = ConfigDict(
        validate_assignment=True
    )

    class Meta(TableMeta):
        table_name = "pipeline_config"
        partition_key = "pipeline_id"
        sort_key = None
        gsis = [
            GSIDefinition(
                name="ActivePipelinesIndex",
                partition_key="is_active",
                sort_key="updated_at"
            ),
            GSIDefinition(
                name="EnvironmentIndex", 
                partition_key="environment",
                sort_key="created_at"
            )
        ]


# =============================================================================
# Table Configuration Domain
# =============================================================================

class TableType(str, Enum):
    """Supported table types in data processing pipelines."""
    SOURCE = "source"
    DESTINATION = "destination"
    LOOKUP = "lookup"
    INTERMEDIATE = "intermediate"


class DataFormat(str, Enum):
    """Supported data formats for table storage."""
    PARQUET = "parquet"
    JSON = "json"
    CSV = "csv"
    AVRO = "avro"
    DELTA = "delta"


class TableConfig(DynamoDBMixin, DateTimeMixin, BaseModel):
    """
    Core domain model for table configuration data.
    
    Represents the complete configuration of a data table including schema,
    location, processing options, data quality settings, and lifecycle management.
    
    Uses DateTimeMixin for consistent datetime validation and serialization.
    """

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

    # Statistics (updated by pipeline runs) - datetime validation/serialization handled by DateTimeMixin
    last_updated_data: Optional[datetime] = Field(None, description="Last data update timestamp")
    record_count: Optional[int] = Field(None, description="Approximate record count")
    size_bytes: Optional[int] = Field(None, description="Table size in bytes")

    # Timestamps - datetime validation/serialization handled by DateTimeMixin
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Creation timestamp")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Last update timestamp")
    created_by: Optional[str] = Field(None, description="User who created the configuration")
    updated_by: Optional[str] = Field(None, description="User who last updated the configuration")

    model_config = ConfigDict(
        validate_assignment=True
    )

    class Meta(TableMeta):
        table_name = "table_config"
        partition_key = "table_id"
        sort_key = None
        gsis = [
            GSIDefinition(
                name="PipelineTablesIndex",
                partition_key="pipeline_id",
                sort_key="table_type"
            ),
            GSIDefinition(
                name="TableTypeIndex",
                partition_key="table_type",
                sort_key="pipeline_id"
            )
        ]


# =============================================================================
# Pipeline Run Log Domain
# =============================================================================

class RunStatus(str, Enum):
    """Pipeline run execution statuses."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class LogLevel(str, Enum):
    """Log severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class StageInfo(DynamoDBMixin, DateTimeMixin, BaseModel):
    """
    Information about a specific stage in a pipeline execution.
    
    Tracks the status, timing, and metrics for individual stages
    within a pipeline run for detailed monitoring and debugging.
    
    Uses DateTimeMixin for consistent datetime validation and serialization.
    """
    stage_name: str = Field(..., description="Name of the stage")
    status: RunStatus = Field(..., description="Status of the stage")
    start_time: Optional[datetime] = Field(None, description="Stage start time")
    end_time: Optional[datetime] = Field(None, description="Stage end time")
    duration_seconds: Optional[Decimal] = Field(None, description="Stage duration in seconds")
    records_processed: Optional[int] = Field(None, description="Number of records processed")
    error_message: Optional[str] = Field(None, description="Error message if stage failed")


class DataQualityResult(BaseModel):
    """
    Result of a data quality check.
    
    Captures the outcome of data quality validations including
    expected vs actual values and detailed error information.
    """
    check_name: str = Field(..., description="Name of the quality check")
    passed: bool = Field(..., description="Whether the check passed")
    expected_value: Optional[Any] = Field(None, description="Expected value")
    actual_value: Optional[Any] = Field(None, description="Actual value")
    error_message: Optional[str] = Field(None, description="Error message if check failed")


class PipelineRunLog(DynamoDBMixin, DateTimeMixin, BaseModel):
    """
    Core domain model for pipeline execution logs.
    
    Comprehensive tracking of pipeline runs including timing, metrics,
    resource usage, data quality results, and error information.
    
    Uses DateTimeMixin for consistent datetime validation and serialization.
    """

    run_id: str = Field(..., description="Unique identifier for this pipeline run")
    pipeline_id: str = Field(..., description="Pipeline identifier")

    # Run information
    status: RunStatus = Field(..., description="Current status of the run")
    trigger_type: str = Field(..., description="What triggered the run (schedule, manual, event)")

    # Timing information - datetime validation/serialization handled by DateTimeMixin
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

    # Timestamps - datetime validation/serialization handled by DateTimeMixin
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Log creation timestamp")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Last update timestamp")

    model_config = ConfigDict(
        validate_assignment=True
    )

    class Meta(TableMeta):
        table_name = "pipeline_run_logs"
        partition_key = "run_id"
        sort_key = "pipeline_id"  # Based on composite key usage patterns found
        gsis = [
            GSIDefinition(
                name="PipelineRunsIndex",
                partition_key="pipeline_id",
                sort_key="start_time"
            ),
            GSIDefinition(
                name="StatusRunsIndex",
                partition_key="status",
                sort_key="start_time"
            )
        ]