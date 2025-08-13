"""
Handler Layer for DynamoDB Wrapper

This module contains the application layer handlers that implement the Command Query
Responsibility Segregation (CQRS) pattern for DynamoDB operations.

The handler layer:
- Coordinates between domain models and infrastructure
- Implements business logic and validation
- Handles timezone conversion at architectural boundaries
- Provides optimized read and write operations

Organization:
- Each domain has its own subdirectory (pipeline_config, table_config, pipeline_run_logs)
- Each domain follows CQRS with queries.py (read) and commands.py (write)
- Read handlers optimize for query performance and data projection
- Write handlers optimize for data consistency and validation

Architecture:
handlers/ (this layer) -> core/ (infrastructure) -> DynamoDB
handlers/ (this layer) <- models/ (domain models)
"""

# Re-export all handler classes for backward compatibility
from .pipeline_config.queries import PipelineConfigReadApi
from .pipeline_config.commands import PipelineConfigWriteApi
from .pipeline_run_logs.queries import PipelineRunLogsReadApi
from .pipeline_run_logs.commands import PipelineRunLogsWriteApi
from .table_config.queries import TableConfigReadApi
from .table_config.commands import TableConfigWriteApi

__all__ = [
    'PipelineConfigReadApi',
    'PipelineConfigWriteApi',
    'PipelineRunLogsReadApi', 
    'PipelineRunLogsWriteApi',
    'TableConfigReadApi',
    'TableConfigWriteApi',
]