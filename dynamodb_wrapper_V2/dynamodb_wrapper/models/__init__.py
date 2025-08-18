# Base mixins and utilities
from .base import (
    DateTimeMixin,
    DynamoDBMixin,
)

# Core domain models (consolidated from 3 separate files)
from .domain_models import (
    # Pipeline Configuration Domain
    PipelineConfig,
    
    # Table Configuration Domain
    TableConfig,
    TableType,
    DataFormat,
    
    # Pipeline Run Log Domain
    PipelineRunLog,
    RunStatus,
    LogLevel,
    StageInfo,
    DataQualityResult,
)

# New CQRS-optimized models
from .views import (
    PipelineConfigView,
    PipelineConfigSummaryView,
    TableConfigView,
    TableConfigSummaryView,
    PipelineRunLogView,
    PipelineRunLogSummaryView,
)
from .dtos import (
    PipelineConfigUpsert,
    TableConfigUpsert,
    PipelineRunLogUpsert,
    PipelineRunLogStatusUpdate,
)

__all__ = [
    # Base mixins and utilities
    "DateTimeMixin",
    "DynamoDBMixin",
    
    # Original models
    "PipelineConfig",
    "TableConfig", 
    "TableType",
    "DataFormat",
    "PipelineRunLog",
    "RunStatus",
    "LogLevel",
    "StageInfo",
    "DataQualityResult",
    
    # CQRS Read Models (Views)
    "PipelineConfigView",
    "PipelineConfigSummaryView",
    "TableConfigView", 
    "TableConfigSummaryView",
    "PipelineRunLogView",
    "PipelineRunLogSummaryView",
    
    # CQRS Write Models (DTOs)
    "PipelineConfigUpsert",
    "TableConfigUpsert",
    "PipelineRunLogUpsert",
    "PipelineRunLogStatusUpdate",
]
