"""
Pipeline Run Logs CQRS APIs

This module provides separate read and write APIs for pipeline run logs,
following Command Query Responsibility Segregation (CQRS) principles.

Read API:
- GSI queries for pipeline and status filtering with time ordering
- Time range queries with efficient KeyConditionExpression
- Projection support for minimal data transfer
- Proper pagination with last_key tokens
- Aggregated statistics and counts

Write API:
- Conditional expressions for data integrity
- Atomic status transitions with duration calculations
- List operations for stage management
- Batch operations for bulk processing
- Transaction support for pipeline-run consistency

Usage:
    from .queries import PipelineRunLogsReadApi
    from .commands import PipelineRunLogsWriteApi
    
    read_api = PipelineRunLogsReadApi(config)
    write_api = PipelineRunLogsWriteApi(config)
"""

from .queries import PipelineRunLogsReadApi
from .commands import PipelineRunLogsWriteApi

__all__ = [
    "PipelineRunLogsReadApi",
    "PipelineRunLogsWriteApi",
]