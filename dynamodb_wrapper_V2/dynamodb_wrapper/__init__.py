from .config import DynamoDBConfig
from .exceptions import (
    ConflictError,
    ConnectionError,
    DynamoDBWrapperError,
    ItemNotFoundError,
    ValidationError,
)
from .models import (
    # Original models
    PipelineConfig,
    PipelineRunLog,
    TableConfig,
    # Enums
    RunStatus,
    LogLevel,
    TableType,
    DataFormat,
    # Model components
    StageInfo,
    DataQualityResult,
    # CQRS View models
    PipelineConfigView,
    PipelineConfigSummaryView,
    TableConfigView,
    TableConfigSummaryView,
    PipelineRunLogView,
    PipelineRunLogSummaryView,
    # CQRS DTOs
    PipelineConfigUpsert,
    TableConfigUpsert,
    PipelineRunLogUpsert,
    PipelineRunLogStatusUpdate,
)
from .core import (
    # TableGateway architecture
    TableGateway,
    create_table_gateway,
)
from .handlers.pipeline_config import (
    # Pipeline Config CQRS APIs
    PipelineConfigReadApi,
    PipelineConfigWriteApi,
)
from .handlers.table_config import (
    # Table Config CQRS APIs
    TableConfigReadApi,
    TableConfigWriteApi,
)
from .handlers.pipeline_run_logs import (
    # Pipeline Run Logs CQRS APIs
    PipelineRunLogsReadApi,
    PipelineRunLogsWriteApi,
)

__version__ = "1.0.0"
__all__ = [
    # Configuration
    "DynamoDBConfig",
    
    # Exceptions
    "ConflictError",
    "ConnectionError",
    "DynamoDBWrapperError",
    "ItemNotFoundError",
    "ValidationError",
    
    # Original models
    "PipelineConfig",
    "TableConfig",
    "PipelineRunLog",
    
    # Enums
    "RunStatus",
    "LogLevel",
    "TableType",
    "DataFormat",
    
    # Model components
    "StageInfo",
    "DataQualityResult",
    
    # CQRS View models (optimized for reads)
    "PipelineConfigView",
    "PipelineConfigSummaryView",
    "TableConfigView",
    "TableConfigSummaryView", 
    "PipelineRunLogView",
    "PipelineRunLogSummaryView",
    
    # CQRS DTOs (optimized for writes)
    "PipelineConfigUpsert",
    "TableConfigUpsert",
    "PipelineRunLogUpsert",
    "PipelineRunLogStatusUpdate",
    
    # TableGateway architecture
    "TableGateway",
    "create_table_gateway",
    
    # CQRS APIs
    "PipelineConfigReadApi",
    "PipelineConfigWriteApi",
    "TableConfigReadApi",
    "TableConfigWriteApi",
    "PipelineRunLogsReadApi",
    "PipelineRunLogsWriteApi",
]
