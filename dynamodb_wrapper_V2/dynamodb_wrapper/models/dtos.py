"""
Write-Optimized DTOs (Data Transfer Objects) for CQRS

These models represent the write side of CQRS, optimized for:
- Input validation and data integrity
- Create and update operations
- Strong typing and constraints
- Clear write-only intent

Key benefits:
- Comprehensive validation rules prevent bad data
- Required field enforcement for data integrity
- Custom validators for business logic
- Clear intent - these are for write operations only
- Separate concerns from read models
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .domain_models import RunStatus, LogLevel, DataQualityResult, TableType, DataFormat


class PipelineConfigUpsert(BaseModel):
    """
    Write-optimized DTO for pipeline configuration create/update operations.
    
    Includes comprehensive validation and all fields needed for:
    - Pipeline creation with full configuration
    - Updates with validation rules
    - Business logic enforcement
    
    Features:
    - Strong validation for required fields
    - Business rule enforcement
    - Environment-specific constraints
    - Version validation
    """
    
    pipeline_id: str = Field(..., min_length=1, max_length=128, pattern=r'^[a-zA-Z0-9_-]+$',
                              description="Unique identifier for the pipeline (alphanumeric, underscore, hyphen only)")
    pipeline_name: str = Field(..., min_length=1, max_length=256, description="Human-readable name of the pipeline")
    description: Optional[str] = Field(None, max_length=1000, description="Description of the pipeline")
    
    # Configuration settings (complete for write operations)
    config: Dict[str, Any] = Field(default_factory=dict, description="Pipeline configuration parameters")
    
    # Source and destination information (required for writes)
    source_type: str = Field(..., min_length=1, max_length=50, description="Type of data source")
    source_config: Dict[str, Any] = Field(default_factory=dict, description="Source configuration parameters")
    
    destination_type: str = Field(..., min_length=1, max_length=50, description="Type of destination")
    destination_config: Dict[str, Any] = Field(default_factory=dict, description="Destination configuration parameters")
    
    # Scheduling and execution
    schedule_expression: Optional[str] = Field(None, max_length=256, description="Cron expression for scheduling")
    is_active: bool = Field(True, description="Whether the pipeline is active")
    
    # Environment and deployment (validated)
    environment: str = Field(default="dev", description="Deployment environment")
    version: str = Field(default="1.0.0", pattern=r'^\d+\.\d+\.\d+$', description="Pipeline version (semantic versioning)")
    
    # Spark specific configurations (complete for execution)
    spark_config: Dict[str, Any] = Field(default_factory=dict, description="Spark-specific configuration")
    
    # Resource requirements (validated ranges)
    cpu_cores: Optional[int] = Field(None, ge=1, le=1000, description="Number of CPU cores required")
    memory_gb: Optional[Decimal] = Field(None, ge=0.5, le=1000.0, description="Memory requirement in GB")
    
    # Metadata
    tags: Dict[str, str] = Field(default_factory=dict, description="Key-value tags for the pipeline")
    
    # Audit fields (can be set on create/update)
    created_by: Optional[str] = Field(None, max_length=128, description="User who created the pipeline")
    updated_by: Optional[str] = Field(None, max_length=128, description="User who last updated the pipeline")
    
    @field_validator('environment')
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment is one of allowed values."""
        allowed_envs = {'dev', 'staging', 'prod', 'test'}
        if v not in allowed_envs:
            raise ValueError(f"Environment must be one of {allowed_envs}")
        return v
    
    @field_validator('source_type', 'destination_type')
    @classmethod
    def validate_data_types(cls, v: str) -> str:
        """Validate source/destination types."""
        allowed_types = {'s3', 'database', 'api', 'warehouse', 'kafka', 'file', 'stream'}
        if v not in allowed_types:
            raise ValueError(f"Type must be one of {allowed_types}")
        return v
    
    @field_validator('schedule_expression')
    @classmethod
    def validate_cron_expression(cls, v: Optional[str]) -> Optional[str]:
        """Basic validation for cron expression format."""
        if v is None:
            return v
        # Basic check for cron format (6 parts: second, minute, hour, day, month, day-of-week)
        if v and len(v.split()) not in [5, 6]:
            raise ValueError("Cron expression must have 5 or 6 parts")
        return v
    
    @model_validator(mode='after')
    def validate_resource_requirements(self) -> 'PipelineConfigUpsert':
        """Validate resource requirements are reasonable."""
        if self.cpu_cores and self.memory_gb:
            # Basic sanity check: at least 0.5GB per CPU core
            if self.memory_gb < (self.cpu_cores * 0.5):
                raise ValueError("Memory allocation should be at least 0.5GB per CPU core")
        return self


class TableConfigUpsert(BaseModel):
    """
    Write-optimized DTO for table configuration create/update operations.
    
    Includes comprehensive validation and all fields needed for:
    - Table creation with full schema and configuration
    - Updates with validation rules
    - Data quality and processing configuration
    
    Features:
    - Strong validation for table identifiers and paths
    - Schema validation for data quality
    - Performance optimization validation
    - Lifecycle management validation
    """
    
    table_id: str = Field(..., min_length=1, max_length=128, pattern=r'^[a-zA-Z0-9_-]+$',
                           description="Unique identifier for the table")
    pipeline_id: str = Field(..., min_length=1, max_length=128, pattern=r'^[a-zA-Z0-9_-]+$',
                              description="Pipeline this table belongs to")
    table_name: str = Field(..., min_length=1, max_length=256, description="Physical table name")
    
    # Table classification (required and validated)
    table_type: TableType = Field(..., description="Type of table in the pipeline")
    data_format: DataFormat = Field(..., description="Data format of the table")
    
    # Schema information (complete for data validation)
    schema_definition: Dict[str, Any] = Field(default_factory=dict, description="Table schema definition")
    partition_columns: List[str] = Field(default_factory=list, description="Partition columns")
    primary_key_columns: List[str] = Field(default_factory=list, description="Primary key columns")
    
    # Location and connection (validated)
    location: str = Field(..., min_length=1, max_length=1000, description="Physical location/path of the table")
    connection_config: Dict[str, Any] = Field(default_factory=dict, description="Connection configuration")
    
    # Processing configuration (complete for execution)
    read_options: Dict[str, Any] = Field(default_factory=dict, description="Options for reading the table")
    write_options: Dict[str, Any] = Field(default_factory=dict, description="Options for writing to the table")
    
    # Data quality and validation (comprehensive)
    validation_rules: Dict[str, Any] = Field(default_factory=dict, description="Data validation rules")
    data_quality_checks: List[Dict[str, Any]] = Field(default_factory=list, description="Data quality check configurations")
    
    # Performance optimization (validated ranges)
    caching_enabled: bool = Field(False, description="Whether caching is enabled for this table")
    cache_level: Optional[str] = Field(None, description="Spark cache level")
    
    # Lifecycle management (validated ranges)
    retention_days: Optional[int] = Field(None, ge=1, le=36500, description="Data retention period in days (max 100 years)")
    archive_after_days: Optional[int] = Field(None, ge=1, le=36500, description="Archive data after specified days")
    
    # Environment and deployment
    environment: str = Field(default="dev", description="Environment")
    is_active: bool = Field(True, description="Whether the table configuration is active")
    
    # Metadata
    description: Optional[str] = Field(None, max_length=1000, description="Table description")
    tags: Dict[str, str] = Field(default_factory=dict, description="Key-value tags")
    
    # Audit fields
    created_by: Optional[str] = Field(None, max_length=128, description="User who created the configuration")
    updated_by: Optional[str] = Field(None, max_length=128, description="User who last updated the configuration")
    
    @field_validator('environment')
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment is one of allowed values."""
        allowed_envs = {'dev', 'staging', 'prod', 'test'}
        if v not in allowed_envs:
            raise ValueError(f"Environment must be one of {allowed_envs}")
        return v
    
    @field_validator('cache_level')
    @classmethod
    def validate_cache_level(cls, v: Optional[str]) -> Optional[str]:
        """Validate Spark cache level."""
        if v is None:
            return v
        allowed_levels = {'MEMORY_ONLY', 'MEMORY_AND_DISK', 'MEMORY_ONLY_SER', 'MEMORY_AND_DISK_SER', 'DISK_ONLY'}
        if v not in allowed_levels:
            raise ValueError(f"Cache level must be one of {allowed_levels}")
        return v
    
    @field_validator('location')
    @classmethod
    def validate_location(cls, v: str) -> str:
        """Basic validation for location paths."""
        if not v.strip():
            raise ValueError("Location cannot be empty")
        # Basic path validation - should start with valid scheme or be absolute path
        valid_schemes = ['s3://', 'hdfs://', 'file://', 'gs://', 'wasbs://', '/']
        if not any(v.startswith(scheme) for scheme in valid_schemes):
            raise ValueError(f"Location must start with one of {valid_schemes}")
        return v
    
    @model_validator(mode='after')
    def validate_lifecycle_settings(self) -> 'TableConfigUpsert':
        """Validate lifecycle management settings."""
        if self.retention_days and self.archive_after_days:
            if self.archive_after_days >= self.retention_days:
                raise ValueError("Archive period must be less than retention period")
        return self


class PipelineRunLogUpsert(BaseModel):
    """
    Write-optimized DTO for pipeline run log create/update operations.
    
    Includes comprehensive validation and all fields needed for:
    - Run log creation with full tracking information
    - Status updates with validation rules
    - Performance and quality metrics
    
    Features:
    - Status transition validation
    - Resource usage validation
    - Data quality validation
    - Comprehensive error handling
    """
    
    run_id: str = Field(..., min_length=1, max_length=128, pattern=r'^[a-zA-Z0-9_-]+$',
                        description="Unique identifier for this pipeline run")
    pipeline_id: str = Field(..., min_length=1, max_length=128, pattern=r'^[a-zA-Z0-9_-]+$',
                              description="Pipeline identifier")
    
    # Run information (validated)
    status: RunStatus = Field(..., description="Current status of the run")
    trigger_type: str = Field(..., min_length=1, max_length=50, description="What triggered the run")
    
    # Timing information (auto-handled but can be overridden)
    start_time: Optional[datetime] = Field(None, description="Run start time")
    end_time: Optional[datetime] = Field(None, description="Run end time")
    duration_seconds: Optional[Decimal] = Field(None, ge=0, description="Total run duration in seconds")
    
    # Data processing metrics (validated ranges)
    total_records_processed: Optional[int] = Field(None, ge=0, description="Total records processed")
    total_records_failed: Optional[int] = Field(None, ge=0, description="Total records that failed processing")
    input_data_size_bytes: Optional[int] = Field(None, ge=0, description="Size of input data in bytes")
    output_data_size_bytes: Optional[int] = Field(None, ge=0, description="Size of output data in bytes")
    
    # Resource usage (validated ranges)
    spark_application_id: Optional[str] = Field(None, max_length=256, description="Spark application ID")
    cpu_hours: Optional[Decimal] = Field(None, ge=0, description="CPU hours consumed")
    memory_usage_gb: Optional[Decimal] = Field(None, ge=0, description="Peak memory usage in GB")
    
    # Data quality (comprehensive)
    data_quality_results: List[DataQualityResult] = Field(default_factory=list, description="Data quality check results")
    data_quality_passed: bool = Field(True, description="Whether all data quality checks passed")
    
    # Error handling (validated lengths)
    error_message: Optional[str] = Field(None, max_length=2000, description="Error message if run failed")
    error_stack_trace: Optional[str] = Field(None, max_length=10000, description="Full error stack trace")
    retry_count: int = Field(0, ge=0, le=10, description="Number of retries attempted (max 10)")
    
    # Configuration and versioning
    pipeline_version: Optional[str] = Field(None, max_length=50, description="Version of pipeline configuration used")
    config_snapshot: Dict[str, Any] = Field(default_factory=dict, description="Snapshot of configuration used")
    
    # Environment information
    environment: str = Field(default="dev", description="Environment where run executed")
    spark_version: Optional[str] = Field(None, max_length=50, description="Spark version used")
    python_version: Optional[str] = Field(None, max_length=50, description="Python version used")
    
    # Lineage and dependencies
    input_tables: List[str] = Field(default_factory=list, description="List of input table IDs")
    output_tables: List[str] = Field(default_factory=list, description="List of output table IDs")
    
    # Logging and monitoring
    log_level: LogLevel = Field(LogLevel.INFO, description="Log level for this run")
    log_messages: List[Dict[str, Any]] = Field(default_factory=list, description="Structured log messages")
    monitoring_urls: Dict[str, str] = Field(default_factory=dict, description="URLs to monitoring dashboards")
    
    # Metadata
    tags: Dict[str, str] = Field(default_factory=dict, description="Key-value tags")
    created_by: Optional[str] = Field(None, max_length=128, description="User who triggered the run")
    
    @field_validator('environment')
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment is one of allowed values."""
        allowed_envs = {'dev', 'staging', 'prod', 'test'}
        if v not in allowed_envs:
            raise ValueError(f"Environment must be one of {allowed_envs}")
        return v
    
    @field_validator('trigger_type')
    @classmethod
    def validate_trigger_type(cls, v: str) -> str:
        """Validate trigger type."""
        allowed_triggers = {'manual', 'schedule', 'event', 'dependency', 'api', 'retry'}
        if v not in allowed_triggers:
            raise ValueError(f"Trigger type must be one of {allowed_triggers}")
        return v
    
    @model_validator(mode='after')
    def validate_timing_and_status(self) -> 'PipelineRunLogUpsert':
        """Validate timing information and status consistency."""
        # If end_time is set, start_time should also be set
        if self.end_time and not self.start_time:
            raise ValueError("start_time must be set when end_time is provided")
        
        # If both times are set, end_time should be after start_time
        if self.start_time and self.end_time:
            if self.end_time <= self.start_time:
                raise ValueError("end_time must be after start_time")
        
        # Terminal statuses should have end_time
        terminal_statuses = {RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED}
        if self.status in terminal_statuses and not self.end_time:
            # Auto-set end_time for terminal statuses
            self.end_time = datetime.now(timezone.utc)
        
        # Failed runs should have error information
        if self.status == RunStatus.FAILED and not self.error_message:
            raise ValueError("Failed runs must include an error_message")
        
        # Validate data consistency
        if self.total_records_failed and self.total_records_processed:
            if self.total_records_failed > self.total_records_processed:
                raise ValueError("total_records_failed cannot exceed total_records_processed")
        
        return self


class PipelineRunLogStatusUpdate(BaseModel):
    """
    Minimal DTO for pipeline run status updates.
    
    Used for:
    - Quick status transitions
    - Progress updates
    - Error reporting
    
    Optimized for frequent write operations.
    """
    
    status: RunStatus = Field(..., description="New status")
    error_message: Optional[str] = Field(None, max_length=2000, description="Error message if failed")
    end_time: Optional[datetime] = Field(None, description="End time if completed")
    updated_by: Optional[str] = Field(None, max_length=128, description="User making the update")
    
    @model_validator(mode='after')
    def validate_status_update(self) -> 'PipelineRunLogStatusUpdate':
        """Validate status update consistency."""
        # Failed runs should have error message
        if self.status == RunStatus.FAILED and not self.error_message:
            raise ValueError("Failed status updates must include an error_message")
        
        # Terminal statuses should have end_time
        terminal_statuses = {RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED}
        if self.status in terminal_statuses and not self.end_time:
            self.end_time = datetime.now(timezone.utc)
        
        return self