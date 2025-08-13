"""
Core infrastructure components for DynamoDB operations.

This module contains the foundational components used across all domain modules:
- TableGateway: Thin wrapper over boto3 DynamoDB operations
- Factory functions for creating gateways
"""

from .table_gateway import TableGateway, create_table_gateway

__all__ = [
    "TableGateway",
    "create_table_gateway",
]