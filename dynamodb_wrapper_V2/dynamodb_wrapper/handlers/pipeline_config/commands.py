"""
Pipeline Configuration Write API

This module provides optimized write operations for pipeline configurations using:
- Conditional expressions for data integrity (attribute_exists/not_exists)
- Atomic updates with UpdateExpression
- Batch operations for bulk processing
- Transaction support for multi-item consistency

All write operations include safety checks and optimistic locking where appropriate.
"""

import logging
import time
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from ...config import DynamoDBConfig
from ...exceptions import ItemNotFoundError, ValidationError, ConnectionError, ConflictError
from ...models import PipelineConfig, PipelineConfigUpsert
from ...utils import model_to_item
from ...core import create_table_gateway

logger = logging.getLogger(__name__)


class PipelineConfigWriteApi:
    """
    Write-only API for pipeline configuration mutations.
    
    Provides safe, atomic operations with:
    - Conditional expressions to prevent race conditions
    - Partial updates using UpdateExpression
    - Batch operations for bulk processing
    - Transaction support for multi-table consistency
    """

    def __init__(self, config: DynamoDBConfig):
        """Initialize write API with configuration."""
        self.config = config
        self.gateway = create_table_gateway(config, "pipeline_config")

    def create_pipeline(
        self,
        pipeline_data: PipelineConfigUpsert,
        condition_expression=None
    ) -> PipelineConfig:
        """
        Create new pipeline configuration using validated DTO.
        
        DynamoDB Operation: PutItem with ConditionExpression
        Default Condition: attribute_not_exists(pipeline_id) - prevents overwriting
        
        Args:
            pipeline_data: Validated PipelineConfigUpsert DTO
            condition_expression: Custom condition (defaults to prevent duplicates)
            
        Returns:
            Created PipelineConfig instance
            
        Raises:
            ValidationError: Invalid pipeline data
            ConnectionError: Pipeline ID already exists or DynamoDB error
        """
        try:
            # Convert validated DTO to full model with auto-generated fields
            pipeline_dict = pipeline_data.model_dump()
            pipeline_dict['created_at'] = datetime.now(timezone.utc)
            pipeline_dict['updated_at'] = datetime.now(timezone.utc)
            pipeline = PipelineConfig(**pipeline_dict)
        except Exception as e:
            raise ValidationError(f"Invalid pipeline data: {e}") from e
            
        # Convert to DynamoDB item
        item = model_to_item(pipeline)
        
        # Default condition prevents accidental overwrites
        if condition_expression is None:
            condition_expression = Attr('pipeline_id').not_exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        self.gateway.put_item(item, condition_expression=condition_expression)
        logger.info(f"Created pipeline: {pipeline.pipeline_id}")
        return pipeline

    def update_pipeline(
        self,
        pipeline_id: str,
        updates: Dict[str, Any],
        condition_expression=None
    ) -> Dict[str, Any]:
        """
        Update pipeline configuration fields atomically.
        
        DynamoDB Operation: UpdateItem with ConditionExpression
        Default Condition: attribute_exists(pipeline_id) - ensures item exists
        
        Args:
            pipeline_id: Pipeline identifier
            updates: Dictionary of fields to update
            condition_expression: Custom condition (defaults to require existence)
            
        Returns:
            Updated pipeline attributes
            
        Raises:
            ItemNotFoundError: Pipeline doesn't exist
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
            
            # Convert datetime objects to ISO strings for DynamoDB
            if isinstance(value, datetime):
                expression_values[attr_value] = value.isoformat()
            # Convert boolean to string for GSI compatibility
            elif isinstance(value, bool):
                expression_values[attr_value] = 'true' if value else 'false'
            else:
                expression_values[attr_value] = value
            
        update_expression = "SET " + ", ".join(update_parts)
        
        # Default condition ensures pipeline exists
        if condition_expression is None:
            condition_expression = Attr('pipeline_id').exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        response = self.gateway.update_item(
            key={'pipeline_id': pipeline_id},
            update_expression=update_expression,
            expression_attribute_values=expression_values,
            expression_attribute_names=expression_names,
            condition_expression=condition_expression,
            return_values='ALL_NEW'
        )
        
        logger.info(f"Updated pipeline {pipeline_id}: {list(updates.keys())}")
        return response

    def update_pipeline_status(
        self,
        pipeline_id: str,
        is_active: bool,
        updated_by: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update pipeline active status atomically.
        
        DynamoDB Operation: UpdateItem with optimized field updates
        
        Args:
            pipeline_id: Pipeline identifier
            is_active: New active status
            updated_by: User making the change
            
        Returns:
            Updated pipeline attributes
        """
        updates = {'is_active': is_active}
        if updated_by:
            updates['updated_by'] = updated_by
            
        return self.update_pipeline(pipeline_id, updates)

    def delete_pipeline(
        self,
        pipeline_id: str,
        condition_expression=None
    ) -> bool:
        """
        Delete pipeline configuration.
        
        DynamoDB Operation: DeleteItem with ConditionExpression
        Default Condition: attribute_exists(pipeline_id) - ensures safe deletion
        
        Args:
            pipeline_id: Pipeline identifier
            condition_expression: Custom condition (defaults to require existence)
            
        Returns:
            True if deleted, raises exception if not found
            
        Raises:
            ItemNotFoundError: Pipeline doesn't exist
            ConnectionError: DynamoDB error
        """
        # Default condition ensures safe deletion
        if condition_expression is None:
            condition_expression = Attr('pipeline_id').exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        response = self.gateway.delete_item(
            key={'pipeline_id': pipeline_id},
            condition_expression=condition_expression,
            return_values='ALL_OLD'
        )
        
        if response:
            logger.info(f"Deleted pipeline: {pipeline_id}")
            return True
        return False

    def upsert_pipeline(
        self,
        pipeline_data: PipelineConfigUpsert
    ) -> PipelineConfig:
        """
        Create or update pipeline configuration using validated DTO (upsert operation).
        
        DynamoDB Operation: PutItem without conditions - allows overwrite
        
        Args:
            pipeline_data: Validated PipelineConfigUpsert DTO
            
        Returns:
            PipelineConfig instance
            
        Raises:
            ValidationError: Invalid pipeline data
        """
        try:
            # Convert validated DTO to full model, preserving/setting timestamps
            pipeline_dict = pipeline_data.model_dump()
            now = datetime.now(timezone.utc)
            
            # Check if item already exists to preserve created_at
            existing_item = None
            try:
                response = self.gateway.table.get_item(
                    Key={'pipeline_id': pipeline_data.pipeline_id}
                )
                if 'Item' in response:
                    existing_item = response['Item']
            except Exception:
                pass  # Item doesn't exist or other error - proceed with new timestamps
            
            # Set timestamps based on whether item exists
            if existing_item and 'created_at' in existing_item:
                # Preserve existing created_at for true upsert semantics
                # Convert string back to datetime if needed
                created_at_str = existing_item['created_at']
                if isinstance(created_at_str, str):
                    pipeline_dict['created_at'] = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                else:
                    pipeline_dict['created_at'] = created_at_str
            else:
                # New item - set created_at
                pipeline_dict['created_at'] = now
            
            # Always update the updated_at timestamp
            pipeline_dict['updated_at'] = now
            
            pipeline = PipelineConfig(**pipeline_dict)
        except Exception as e:
            raise ValidationError(f"Invalid pipeline data: {e}") from e
            
        item = model_to_item(pipeline)
        
        self.gateway.put_item(item)  # No condition - allows overwrite
        logger.info(f"Upserted pipeline: {pipeline.pipeline_id}")
        return pipeline

    def upsert_many(
        self,
        pipelines_data: List[PipelineConfigUpsert],
        overwrite_by_pkeys: List[str] = ["pipeline_id"]
    ) -> List[PipelineConfig]:
        """
        Bulk upsert multiple pipeline configurations using validated DTOs.
        
        DynamoDB Operation: BatchWriteItem with put operations
        
        Args:
            pipelines_data: List of validated PipelineConfigUpsert DTOs
            overwrite_by_pkeys: Primary keys that allow overwriting (for safety)
            
        Returns:
            List of created PipelineConfig instances
            
        Raises:
            ValidationError: Invalid pipeline data
        """
        if not pipelines_data:
            return []
            
        # Convert all validated DTOs to full models
        validated_pipelines = []
        now = datetime.now(timezone.utc)
        
        for pipeline_dto in pipelines_data:
            try:
                # Convert DTO to full model with timestamps
                pipeline_dict = pipeline_dto.model_dump()
                if 'created_at' not in pipeline_dict:
                    pipeline_dict['created_at'] = now
                pipeline_dict['updated_at'] = now
                
                pipeline = PipelineConfig(**pipeline_dict)
                validated_pipelines.append(pipeline)
            except Exception as e:
                raise ValidationError(f"Invalid pipeline data for {pipeline_dto.pipeline_id}: {e}") from e
        
        # Use enhanced batch write with retry logic
        return self._batch_write_with_retry(validated_pipelines, max_retries=3)

    def activate_pipelines(
        self,
        pipeline_ids: List[str],
        updated_by: Optional[str] = None
    ) -> int:
        """
        Activate multiple pipelines atomically using transaction.
        
        DynamoDB Operation: TransactWriteItems with conditional updates
        
        Args:
            pipeline_ids: List of pipeline IDs to activate
            updated_by: User making the changes
            
        Returns:
            Number of pipelines activated
            
        Raises:
            ConnectionError: Transaction failed
        """
        if not pipeline_ids:
            return 0
            
        # Build transaction items
        transact_items = []
        update_time = datetime.now(timezone.utc).isoformat()
        
        for pipeline_id in pipeline_ids:
            update_expression = "SET is_active = :active, updated_at = :time"
            expression_values = {
                ':active': 'true',
                ':time': update_time
            }
            
            if updated_by:
                update_expression += ", updated_by = :user"
                expression_values[':user'] = updated_by
                
            transact_items.append({
                'Update': {
                    'TableName': self.gateway.table_name,
                    'Key': {'pipeline_id': pipeline_id},
                    'UpdateExpression': update_expression,
                    'ExpressionAttributeValues': expression_values,
                    'ConditionExpression': 'attribute_exists(pipeline_id)'
                }
            })
        
        # TableGateway maps ClientErrors to appropriate domain exceptions automatically
        self.gateway.transact_write_items(transact_items)
        logger.info(f"Activated {len(pipeline_ids)} pipelines in transaction")
        return len(pipeline_ids)

    def deactivate_pipelines(
        self,
        pipeline_ids: List[str],
        updated_by: Optional[str] = None
    ) -> int:
        """
        Deactivate multiple pipelines atomically using transaction.
        
        DynamoDB Operation: TransactWriteItems with conditional updates
        
        Args:
            pipeline_ids: List of pipeline IDs to deactivate
            updated_by: User making the changes
            
        Returns:
            Number of pipelines deactivated
            
        Raises:
            ConnectionError: Transaction failed
        """
        if not pipeline_ids:
            return 0
            
        # Build transaction items
        transact_items = []
        update_time = datetime.now(timezone.utc).isoformat()
        
        for pipeline_id in pipeline_ids:
            update_expression = "SET is_active = :active, updated_at = :time"
            expression_values = {
                ':active': 'false',
                ':time': update_time
            }
            
            if updated_by:
                update_expression += ", updated_by = :user"
                expression_values[':user'] = updated_by
                
            transact_items.append({
                'Update': {
                    'TableName': self.gateway.table_name,
                    'Key': {'pipeline_id': pipeline_id},
                    'UpdateExpression': update_expression,
                    'ExpressionAttributeValues': expression_values,
                    'ConditionExpression': 'attribute_exists(pipeline_id)'
                }
            })
        
        # TableGateway maps ClientErrors to appropriate domain exceptions automatically
        self.gateway.transact_write_items(transact_items)
        logger.info(f"Deactivated {len(pipeline_ids)} pipelines in transaction")
        return len(pipeline_ids)

    def archive_old_pipelines(
        self,
        environment: str,
        days_inactive: int = 90,
        updated_by: Optional[str] = None
    ) -> int:
        """
        Archive pipelines that have been inactive for specified days.
        
        This is a compound operation that:
        1. Queries inactive pipelines in environment (read API)
        2. Filters by last activity date
        3. Updates archived status (write API)
        
        Args:
            environment: Environment to process
            days_inactive: Days since last activity
            updated_by: User performing archive operation
            
        Returns:
            Number of pipelines archived
        """
        # This would typically use the read API to find candidates
        # then update their status - demonstrating read/write API composition
        from .queries import PipelineConfigReadApi
        
        read_api = PipelineConfigReadApi(self.config)
        cutoff_date = datetime.now(timezone.utc).timestamp() - (days_inactive * 24 * 60 * 60)
        
        # Get inactive pipelines (would need additional filtering logic)
        inactive_pipelines, _ = read_api.query_by_environment_and_status(
            environment=environment,
            is_active=False,
            limit=100
        )
        
        # Filter by activity date and update (simplified for example)
        archived_count = 0
        for pipeline in inactive_pipelines:
            # Update to archived status
            try:
                self.update_pipeline(
                    pipeline.pipeline_id,
                    {'archived': True, 'archived_by': updated_by}
                )
                archived_count += 1
            except Exception as e:
                logger.warning(f"Failed to archive pipeline {pipeline.pipeline_id}: {e}")
                
        return archived_count
        
    def _batch_write_with_retry(
        self, 
        pipelines: List[PipelineConfig], 
        max_retries: int = 3
    ) -> List[PipelineConfig]:
        """Batch write with UnprocessedItems retry logic and size validation.
        
        Args:
            pipelines: List of validated PipelineConfig instances
            max_retries: Maximum retry attempts for failed items
            
        Returns:
            List of successfully written pipelines
            
        Raises:
            ValidationError: Item size exceeds 400KB limit
            ConnectionError: Batch write failed after retries
        """
        if not pipelines:
            return []
            
        # Validate item sizes before processing
        for pipeline in pipelines:
            item = model_to_item(pipeline)
            item_size = self._calculate_item_size(item)
            if item_size > 400 * 1024:  # 400KB limit
                raise ValidationError(
                    f"Item size {item_size} bytes exceeds 400KB DynamoDB limit for pipeline {pipeline.pipeline_id}"
                )
        
        # Process in chunks of 25 (DynamoDB batch limit)
        chunk_size = 25
        all_successful = []
        
        for i in range(0, len(pipelines), chunk_size):
            chunk = pipelines[i:i + chunk_size]
            successful_chunk = self._write_chunk_with_retry(chunk, max_retries)
            all_successful.extend(successful_chunk)
            
        logger.info(f"Bulk upserted {len(all_successful)} pipelines")
        return all_successful
        
    def _write_chunk_with_retry(
        self, 
        pipelines: List[PipelineConfig], 
        max_retries: int
    ) -> List[PipelineConfig]:
        """Write a single chunk with retry logic for UnprocessedItems."""
        items_to_write = [model_to_item(pipeline) for pipeline in pipelines]
        pipeline_map = {item['pipeline_id']: pipeline for pipeline, item in zip(pipelines, items_to_write)}
        successful_pipelines = []
        
        for attempt in range(max_retries + 1):
            if not items_to_write:
                break
                
            try:
                # Use low-level batch_write_item for UnprocessedItems handling
                response = self.gateway.dynamodb.batch_write_item(
                    RequestItems={
                        self.gateway.table_name: [
                            {'PutRequest': {'Item': item}} for item in items_to_write
                        ]
                    }
                )
                
                # Track successful items
                unprocessed_items = response.get('UnprocessedItems', {})
                
                if self.gateway.table_name in unprocessed_items:
                    unprocessed_requests = unprocessed_items[self.gateway.table_name]
                    unprocessed_count = len(unprocessed_requests)
                    
                    # Identify successful items (those not in unprocessed)
                    unprocessed_ids = {
                        req['PutRequest']['Item']['pipeline_id'] 
                        for req in unprocessed_requests
                    }
                    
                    for item in items_to_write:
                        if item['pipeline_id'] not in unprocessed_ids:
                            successful_pipelines.append(pipeline_map[item['pipeline_id']])
                    
                    # Prepare unprocessed items for retry
                    items_to_write = [req['PutRequest']['Item'] for req in unprocessed_requests]
                    
                    if attempt < max_retries:
                        # Exponential backoff with jitter
                        delay = (2 ** attempt) + (time.time() % 1)
                        logger.warning(
                            f"Retrying {unprocessed_count} unprocessed items after {delay:.2f}s "
                            f"(attempt {attempt + 1}/{max_retries + 1})"
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"Failed to process {unprocessed_count} items after {max_retries} retries")
                        raise ConnectionError(
                            f"Batch write failed for {unprocessed_count} items after {max_retries} retries"
                        )
                else:
                    # All items processed successfully
                    for item in items_to_write:
                        successful_pipelines.append(pipeline_map[item['pipeline_id']])
                    break
                    
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ProvisionedThroughputExceededException' and attempt < max_retries:
                    # Backoff for throttling
                    delay = (2 ** attempt) * 2  # Longer delay for throttling
                    logger.warning(f"Throttled, backing off for {delay}s")
                    time.sleep(delay)
                else:
                    logger.error(f"Batch write error: {e}")
                    raise ConnectionError(f"Batch write failed: {e}") from e
                    
        return successful_pipelines
        
    def _calculate_item_size(self, item: Dict[str, Any]) -> int:
        """Calculate approximate DynamoDB item size in bytes.
        
        This is an approximation based on DynamoDB's size calculation rules:
        - Attribute names and values count toward item size
        - Numbers stored as strings for calculation
        """
        import json
        # Convert to JSON and measure byte size (close approximation)
        json_str = json.dumps(item, default=str, separators=(',', ':'))
        return len(json_str.encode('utf-8'))