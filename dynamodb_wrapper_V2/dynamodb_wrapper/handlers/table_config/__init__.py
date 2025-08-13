"""
Table Configuration CQRS APIs

This module provides separate read and write APIs for table configurations,
following Command Query Responsibility Segregation (CQRS) principles.

Read API:
- GSI queries for pipeline and table type filtering
- Projection support for minimal data transfer
- Proper pagination with last_key tokens
- Server-side filtering with FilterExpression

Write API:
- Conditional expressions for data integrity
- Atomic updates and transactions
- Batch operations for bulk processing
- Statistics updates with optimized expressions

Usage:
    from .queries import TableConfigReadApi
    from .commands import TableConfigWriteApi
    
    read_api = TableConfigReadApi(config)
    write_api = TableConfigWriteApi(config)
"""

from .queries import TableConfigReadApi
from .commands import TableConfigWriteApi

__all__ = [
    "TableConfigReadApi",
    "TableConfigWriteApi",
]