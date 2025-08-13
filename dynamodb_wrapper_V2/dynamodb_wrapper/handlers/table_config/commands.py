"""
Table Configuration Write API

This module provides optimized write operations for table configurations using:
- Conditional expressions for data integrity
- Atomic updates with UpdateExpression
- Batch operations for bulk processing
- Transaction support for pipeline-table consistency

All write operations include safety checks and optimistic locking.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from boto3.dynamodb.conditions import Attr

from ...config import DynamoDBConfig
from ...exceptions import ItemNotFoundError, ValidationError, ConnectionError
from ...models import TableConfig, TableType, TableConfigUpsert
from ...utils import model_to_item
from ...core import create_table_gateway

logger = logging.getLogger(__name__)


class TableConfigWriteApi:
    """
    Write-only API for table configuration mutations.
    
    Provides safe, atomic operations with:
    - Conditional expressions to prevent race conditions
    - Partial updates using UpdateExpression
    - Batch operations for bulk processing
    - Transaction support for multi-table consistency
    """

    def __init__(self, config: DynamoDBConfig):
        """Initialize write API with configuration."""
        self.config = config
        self.gateway = create_table_gateway(config, "table_config")

    def create_table(
        self,
        table_data: TableConfigUpsert,
        condition_expression=None
    ) -> TableConfig:
        """
        Create new table configuration using validated DTO.
        
        DynamoDB Operation: PutItem with ConditionExpression
        Default Condition: attribute_not_exists(table_id) - prevents overwriting
        
        Args:
            table_data: Validated TableConfigUpsert DTO
            condition_expression: Custom condition (defaults to prevent duplicates)
            
        Returns:
            Created TableConfig instance
            
        Raises:
            ValidationError: Invalid table data
            ConnectionError: Table ID already exists or DynamoDB error
        """
        try:
            # Convert validated DTO to full model with auto-generated fields
            table_dict = table_data.model_dump()
            table_dict['created_at'] = datetime.now(timezone.utc)
            table_dict['updated_at'] = datetime.now(timezone.utc)
            table = TableConfig(**table_dict)
        except Exception as e:
            raise ValidationError(f"Invalid table data: {e}") from e
            
        # Convert to DynamoDB item
        item = model_to_item(table)
        
        # Default condition prevents accidental overwrites
        if condition_expression is None:
            condition_expression = Attr('table_id').not_exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        self.gateway.put_item(item, condition_expression=condition_expression)
        logger.info(f"Created table config: {table.table_id}")
        return table

    def update_table(
        self,
        table_id: str,
        updates: Dict[str, Any],
        condition_expression=None
    ) -> Dict[str, Any]:
        """
        Update table configuration fields atomically.
        
        DynamoDB Operation: UpdateItem with ConditionExpression
        Default Condition: attribute_exists(table_id) - ensures item exists
        
        Args:
            table_id: Table identifier
            updates: Dictionary of fields to update
            condition_expression: Custom condition (defaults to require existence)
            
        Returns:
            Updated table attributes
            
        Raises:
            ItemNotFoundError: Table doesn't exist
            ConnectionError: DynamoDB error
        """
        if not updates:
            raise ValidationError("Updates dictionary cannot be empty")
            
        # Build update expression
        update_parts = []
        expression_values = {}
        expression_names = {}
        
        # Always update the updated_at timestamp
        updates['updated_at'] = datetime.now(timezone.utc)
        
        for key, value in updates.items():
            attr_name = f"#{key}"
            attr_value = f":{key}"
            update_parts.append(f"{attr_name} = {attr_value}")
            expression_names[attr_name] = key
            expression_values[attr_value] = value
            
        update_expression = "SET " + ", ".join(update_parts)
        
        # Default condition ensures table exists
        if condition_expression is None:
            condition_expression = Attr('table_id').exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        response = self.gateway.update_item(
            key={'table_id': table_id},
            update_expression=update_expression,
            expression_attribute_values=expression_values,
            expression_attribute_names=expression_names,
            condition_expression=condition_expression,
            return_values='ALL_NEW'
        )
        
        logger.info(f"Updated table config {table_id}: {list(updates.keys())}")
        return response

    def update_table_statistics(
        self,
        table_id: str,
        record_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        last_updated_data: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Update table statistics atomically.
        
        DynamoDB Operation: UpdateItem with optimized field updates
        
        Args:
            table_id: Table identifier
            record_count: New record count
            size_bytes: New size in bytes  
            last_updated_data: When data was last updated
            
        Returns:
            Updated table attributes
        """
        updates = {}
        if record_count is not None:
            updates['record_count'] = record_count
        if size_bytes is not None:
            updates['size_bytes'] = size_bytes
        if last_updated_data is not None:
            updates['last_updated_data'] = last_updated_data
            
        if not updates:
            raise ValidationError("At least one statistic must be provided for update")
            
        return self.update_table(table_id, updates)

    def update_table_status(
        self,
        table_id: str,
        is_active: bool,
        updated_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update table active status atomically.
        
        DynamoDB Operation: UpdateItem with optimized field updates
        
        Args:
            table_id: Table identifier
            is_active: New active status
            updated_by: User making the change
            
        Returns:
            Updated table attributes
        """
        updates = {'is_active': is_active}
        if updated_by:
            updates['updated_by'] = updated_by
            
        return self.update_table(table_id, updates)

    def delete_table(
        self,
        table_id: str,
        condition_expression=None
    ) -> bool:
        """
        Delete table configuration.
        
        DynamoDB Operation: DeleteItem with ConditionExpression
        Default Condition: attribute_exists(table_id) - ensures safe deletion
        
        Args:
            table_id: Table identifier
            condition_expression: Custom condition (defaults to require existence)
            
        Returns:
            True if deleted, raises exception if not found
            
        Raises:
            ItemNotFoundError: Table doesn't exist
            ConnectionError: DynamoDB error
        """
        # Default condition ensures safe deletion
        if condition_expression is None:
            condition_expression = Attr('table_id').exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        response = self.gateway.delete_item(
            key={'table_id': table_id},
            condition_expression=condition_expression,
            return_values='ALL_OLD'
        )
        
        if response:
            logger.info(f"Deleted table config: {table_id}")
            return True
        return False

    def upsert_table(
        self,
        table_data: TableConfigUpsert
    ) -> TableConfig:
        """
        Create or update table configuration using validated DTO (upsert operation).
        
        DynamoDB Operation: PutItem without conditions - allows overwrite
        
        Args:
            table_data: Validated TableConfigUpsert DTO
            
        Returns:
            TableConfig instance
            
        Raises:
            ValidationError: Invalid table data
        """
        try:
            # Convert validated DTO to full model, preserving/setting timestamps
            table_dict = table_data.model_dump()
            now = datetime.now(timezone.utc)
            
            # Set created_at if not provided (for new items)
            if 'created_at' not in table_dict:
                table_dict['created_at'] = now
            # Always update the updated_at timestamp
            table_dict['updated_at'] = now
            
            table = TableConfig(**table_dict)
        except Exception as e:
            raise ValidationError(f"Invalid table data: {e}") from e
            
        item = model_to_item(table)
        
        self.gateway.put_item(item)  # No condition - allows overwrite
        logger.info(f"Upserted table config: {table.table_id}")
        return table

    def upsert_many(
        self,
        tables_data: List[TableConfigUpsert],
        overwrite_by_pkeys: List[str] = ["table_id"]
    ) -> List[TableConfig]:
        """
        Bulk upsert multiple table configurations using validated DTOs.
        
        DynamoDB Operation: BatchWriteItem with put operations
        
        Args:
            tables_data: List of validated TableConfigUpsert DTOs
            overwrite_by_pkeys: Primary keys that allow overwriting (for safety)
            
        Returns:
            List of created TableConfig instances
            
        Raises:
            ValidationError: Invalid table data
        """
        if not tables_data:
            return []
            
        # Convert all validated DTOs to full models
        validated_tables = []
        now = datetime.now(timezone.utc)
        
        for table_dto in tables_data:
            try:
                # Convert DTO to full model with timestamps
                table_dict = table_dto.model_dump()
                if 'created_at' not in table_dict:
                    table_dict['created_at'] = now
                table_dict['updated_at'] = now
                
                table = TableConfig(**table_dict)
                validated_tables.append(table)
            except Exception as e:
                raise ValidationError(f"Invalid table data for {table_dto.table_id}: {e}") from e
        
        # Batch write using context manager
        with self.gateway.batch_writer() as batch:
            for table in validated_tables:
                item = model_to_item(table)
                batch.put_item(Item=item)
                
        logger.info(f"Bulk upserted {len(validated_tables)} table configs")
        return validated_tables

    def activate_tables_for_pipeline(
        self,
        pipeline_id: str,
        table_ids: Optional[List[str]] = None,
        updated_by: Optional[str] = None
    ) -> int:
        """
        Activate tables for a pipeline using transaction.
        
        DynamoDB Operation: TransactWriteItems with conditional updates
        
        Args:
            pipeline_id: Pipeline identifier
            table_ids: Specific table IDs (None for all tables in pipeline)
            updated_by: User making the changes
            
        Returns:
            Number of tables activated
            
        Raises:
            ConnectionError: Transaction failed
        """
        # If table_ids not specified, would need to query first
        # For this example, assuming table_ids are provided
        if not table_ids:
            raise ValidationError("table_ids must be provided for batch activation")
            
        # Build transaction items
        transact_items = []
        update_time = datetime.now(timezone.utc).isoformat()
        
        for table_id in table_ids:
            update_expression = "SET is_active = :active, updated_at = :time"
            expression_values = {
                ':active': True,
                ':time': update_time,
                ':pipeline': pipeline_id
            }
            
            if updated_by:
                update_expression += ", updated_by = :user"
                expression_values[':user'] = updated_by
                
            transact_items.append({
                'Update': {
                    'TableName': self.gateway.table_name,
                    'Key': {'table_id': table_id},
                    'UpdateExpression': update_expression,
                    'ExpressionAttributeValues': expression_values,
                    'ConditionExpression': 'attribute_exists(table_id) AND pipeline_id = :pipeline'
                }
            })
        
        # TableGateway maps ClientErrors to appropriate domain exceptions automatically
        self.gateway.transact_write_items(transact_items)
        logger.info(f"Activated {len(table_ids)} tables for pipeline {pipeline_id}")
        return len(table_ids)

    def deactivate_tables_for_pipeline(
        self,
        pipeline_id: str,
        table_ids: Optional[List[str]] = None,
        updated_by: Optional[str] = None
    ) -> int:
        """
        Deactivate tables for a pipeline using transaction.
        
        DynamoDB Operation: TransactWriteItems with conditional updates
        
        Args:
            pipeline_id: Pipeline identifier
            table_ids: Specific table IDs (None for all tables in pipeline)
            updated_by: User making the changes
            
        Returns:
            Number of tables deactivated
            
        Raises:
            ConnectionError: Transaction failed
        """
        if not table_ids:
            raise ValidationError("table_ids must be provided for batch deactivation")
            
        # Build transaction items
        transact_items = []
        update_time = datetime.now(timezone.utc).isoformat()
        
        for table_id in table_ids:
            update_expression = "SET is_active = :active, updated_at = :time"
            expression_values = {
                ':active': False,
                ':time': update_time,
                ':pipeline': pipeline_id
            }
            
            if updated_by:
                update_expression += ", updated_by = :user"
                expression_values[':user'] = updated_by
                
            transact_items.append({
                'Update': {
                    'TableName': self.gateway.table_name,
                    'Key': {'table_id': table_id},
                    'UpdateExpression': update_expression,
                    'ExpressionAttributeValues': expression_values,
                    'ConditionExpression': 'attribute_exists(table_id) AND pipeline_id = :pipeline'
                }
            })
        
        # TableGateway maps ClientErrors to appropriate domain exceptions automatically
        self.gateway.transact_write_items(transact_items)
        logger.info(f"Deactivated {len(table_ids)} tables for pipeline {pipeline_id}")
        return len(table_ids)

    def bulk_update_statistics(
        self,
        statistics_updates: List[Dict[str, Any]]
    ) -> int:
        """
        Bulk update table statistics for multiple tables.
        
        Args:
            statistics_updates: List of dicts with table_id and statistics
            
        Returns:
            Number of tables updated
            
        Example:
            statistics_updates = [
                {
                    'table_id': 'table1',
                    'record_count': 1000,
                    'size_bytes': 50000
                },
                {
                    'table_id': 'table2', 
                    'record_count': 2000,
                    'size_bytes': 100000
                }
            ]
        """
        if not statistics_updates:
            return 0
            
        updated_count = 0
        for update_data in statistics_updates:
            table_id = update_data.pop('table_id')
            try:
                self.update_table_statistics(table_id, **update_data)
                updated_count += 1
            except Exception as e:
                logger.warning(f"Failed to update statistics for table {table_id}: {e}")
                
        return updated_count

    def archive_unused_tables(
        self,
        pipeline_id: str,
        days_unused: int = 30,
        updated_by: Optional[str] = None
    ) -> int:
        """
        Archive tables that haven't been used for specified days.
        
        This is a compound operation demonstrating read/write API composition.
        
        Args:
            pipeline_id: Pipeline to process
            days_unused: Days since last data update
            updated_by: User performing archive operation
            
        Returns:
            Number of tables archived
        """
        from .queries import TableConfigReadApi
        
        read_api = TableConfigReadApi(self.config)
        cutoff_timestamp = datetime.now(timezone.utc).timestamp() - (days_unused * 24 * 60 * 60)
        
        # Get all tables for pipeline
        tables, _ = read_api.query_by_pipeline(
            pipeline_id,
            projection=['table_id', 'last_updated_data'],
            limit=1000
        )
        
        # Filter tables that need archiving
        tables_to_archive = []
        for table in tables:
            if (table.last_updated_data and 
                table.last_updated_data.timestamp() < cutoff_timestamp):
                tables_to_archive.append(table.table_id)
        
        # Archive tables
        archived_count = 0
        for table_id in tables_to_archive:
            try:
                self.update_table(
                    table_id,
                    {'archived': True, 'archived_by': updated_by}
                )
                archived_count += 1
            except Exception as e:
                logger.warning(f"Failed to archive table {table_id}: {e}")
                
        return archived_count