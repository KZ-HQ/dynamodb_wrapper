"""
Pipeline Configuration CQRS APIs

This module provides separate query and command APIs for pipeline configurations,
following Command Query Responsibility Segregation (CQRS) principles.

Queries (Read Operations):
- Optimized for query patterns with GSI usage
- Projection support for minimal data transfer
- Proper pagination with last_key tokens
- No inefficient full table scans

Commands (Write Operations):
- Conditional expressions for data integrity
- Atomic updates and transactions
- Batch operations for bulk processing
- Safe operations with existence checks

Usage:
    from .queries import PipelineConfigReadApi
    from .commands import PipelineConfigWriteApi
    
    read_api = PipelineConfigReadApi(config)
    write_api = PipelineConfigWriteApi(config)
"""

from .queries import PipelineConfigReadApi
from .commands import PipelineConfigWriteApi

__all__ = [
    "PipelineConfigReadApi",
    "PipelineConfigWriteApi",
]