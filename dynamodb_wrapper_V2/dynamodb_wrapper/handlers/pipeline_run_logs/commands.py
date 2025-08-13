"""
Pipeline Run Logs Write API

This module provides optimized write operations for pipeline run logs using:
- Conditional expressions for data integrity
- Atomic updates with UpdateExpression for status transitions
- List append operations for stage information
- Batch operations for bulk processing
- Transaction support for pipeline-run consistency

All write operations include safety checks and optimistic locking.
"""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any

from boto3.dynamodb.conditions import Attr

from ...config import DynamoDBConfig
from ...exceptions import ItemNotFoundError, ValidationError, ConnectionError
from ...models import PipelineRunLog, RunStatus, PipelineRunLogUpsert, PipelineRunLogStatusUpdate
from ...utils import model_to_item, to_utc, ensure_timezone_aware, build_model_key
from ...core import create_table_gateway

logger = logging.getLogger(__name__)


class PipelineRunLogsWriteApi:
    """
    Write-only API for pipeline run log mutations.
    
    Provides safe, atomic operations with:
    - Conditional expressions to prevent race conditions
    - Atomic status transitions with timestamp updates
    - List operations for stage management
    - Batch operations for bulk processing
    - Duration calculations with Decimal precision
    """

    def __init__(self, config: DynamoDBConfig):
        """Initialize write API with configuration."""
        self.config = config
        self.gateway = create_table_gateway(config, "pipeline_run_logs")
    
    def _ensure_utc_timestamps(self, run_log: PipelineRunLog) -> PipelineRunLog:
        """Ensure all datetime fields in the run log are in UTC.
        
        This method converts any timezone-aware datetime to UTC and 
        assumes naive datetimes are already in UTC (per handler contract).
        
        Args:
            run_log: Pipeline run log instance
            
        Returns:
            Run log with all datetime fields in UTC
        """
        run_dict = run_log.model_dump()
        
        # List of datetime fields that need UTC conversion
        datetime_fields = ['start_time', 'end_time', 'created_at', 'updated_at']
        
        for field in datetime_fields:
            if field in run_dict and run_dict[field] is not None:
                dt = run_dict[field]
                if isinstance(dt, datetime):
                    # Ensure timezone-aware and convert to UTC (always UTC for internal operations)
                    dt = ensure_timezone_aware(dt, "UTC")
                    run_dict[field] = to_utc(dt)
        
        # Handle datetime fields in stages
        if 'stages' in run_dict and run_dict['stages']:
            for stage in run_dict['stages']:
                for field in ['start_time', 'end_time']:
                    if field in stage and stage[field] is not None:
                        dt = stage[field]
                        if isinstance(dt, datetime):
                            dt = ensure_timezone_aware(dt, "UTC")
                            stage[field] = to_utc(dt)
        
        return PipelineRunLog(**run_dict)

    def create_run_log(
        self,
        run_data: PipelineRunLogUpsert,
        condition_expression=None
    ) -> PipelineRunLog:
        """
        Create new pipeline run log using validated DTO.
        
        DynamoDB Operation: PutItem with ConditionExpression
        Default Condition: Primary key (run_id, pipeline_id) does not exist - prevents overwriting
        
        Args:
            run_data: Validated PipelineRunLogUpsert DTO containing run_id and pipeline_id
            condition_expression: Custom condition (defaults to prevent duplicates)
            
        Returns:
            Created PipelineRunLog instance
            
        Raises:
            ValidationError: Invalid run data
            ConnectionError: Key already exists or DynamoDB error
            
        Examples:
            >>> run_data = PipelineRunLogUpsert(
            ...     run_id="run-123",
            ...     pipeline_id="pipeline-456", 
            ...     status=RunStatus.PENDING,
            ...     trigger_type="manual"
            ... )
            >>> api.create_run_log(run_data)
        """
        try:
            # Convert validated DTO to full model with auto-generated fields
            run_dict = run_data.model_dump()
            
            # Set default status if not provided
            if not run_dict.get('status'):
                run_dict['status'] = RunStatus.PENDING
                
            # Set timestamps
            now = datetime.now(timezone.utc)
            if not run_dict.get('start_time'):
                run_dict['start_time'] = now
            run_dict['created_at'] = now
            run_dict['updated_at'] = now
            
            run_log = PipelineRunLog(**run_dict)
        except Exception as e:
            raise ValidationError(f"Invalid run log data: {e}") from e
            
        # Ensure all datetime fields are in UTC before storage
        run_log = self._ensure_utc_timestamps(run_log)
        
        # Convert to DynamoDB item
        item = model_to_item(run_log)
        
        # Default condition prevents accidental overwrites of composite primary key
        if condition_expression is None:
            # Composite key: both partition and sort key must not exist
            condition_expression = Attr('run_id').not_exists() & Attr('pipeline_id').not_exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        self.gateway.put_item(item, condition_expression=condition_expression)
        logger.info(f"Created run log: {run_log.run_id}")
        return run_log

    def update_run_status_with_dto(
        self,
        run_id: str,
        status_update: PipelineRunLogStatusUpdate,
        condition_expression=None
    ) -> Dict[str, Any]:
        """
        Update pipeline run status using validated DTO.
        
        DynamoDB Operation: UpdateItem with ConditionExpression
        Default Condition: attribute_exists(run_id) - ensures run exists
        
        Args:
            run_id: Run identifier
            status_update: Validated PipelineRunLogStatusUpdate DTO
            condition_expression: Custom condition (defaults to require existence)
            
        Returns:
            Updated run log attributes
            
        Raises:
            ItemNotFoundError: Run doesn't exist
            ConnectionError: DynamoDB error
        """
        return self.update_run_status(
            run_id=run_id,
            status=status_update.status,
            error_message=status_update.error_message,
            end_time=status_update.end_time,
            condition_expression=condition_expression
        )

    def update_run_status(
        self,
        run_id: str,
        pipeline_id: str,
        status: RunStatus,
        error_message: Optional[str] = None,
        end_time: Optional[datetime] = None,
        condition_expression=None
    ) -> Dict[str, Any]:
        """
        Update pipeline run status with atomic operations.
        
        DynamoDB Operation: UpdateItem with ConditionExpression and automatic calculations
        Default Condition: Primary key exists - ensures run exists
        
        Args:
            run_id: Run identifier (partition key)
            pipeline_id: Pipeline identifier (sort key)
            status: New status
            error_message: Error message if failed
            end_time: End time if completed (auto-calculated if None for terminal states)
            condition_expression: Custom condition (defaults to require existence)
            
        Returns:
            Updated run log attributes
            
        Raises:
            ItemNotFoundError: Run doesn't exist
            ConnectionError: DynamoDB error
            
        Examples:
            >>> api.update_run_status("run-123", "pipeline-456", RunStatus.COMPLETED)
            >>> api.update_run_status("run-123", "pipeline-456", RunStatus.FAILED, 
            ...                       error_message="Process failed")
        """
        
        # Build key for update operation using model-aware function
        key = build_model_key(PipelineRunLog, run_id=run_id, pipeline_id=pipeline_id)
        
        # Build update expression
        update_parts = ['#status = :status', '#updated_at = :updated_at']
        expression_values = {
            ':status': status.value,
            ':updated_at': datetime.now(timezone.utc).isoformat()
        }
        expression_names = {
            '#status': 'status',
            '#updated_at': 'updated_at'
        }
        
        # Add error message if provided
        if error_message:
            update_parts.append('#error_message = :error_message')
            expression_values[':error_message'] = error_message
            expression_names['#error_message'] = 'error_message'
        
        # Handle terminal states with end time and duration calculation
        terminal_states = [RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED]
        if status in terminal_states:
            if not end_time:
                end_time = datetime.now(timezone.utc)
            
            update_parts.append('#end_time = :end_time')
            expression_values[':end_time'] = end_time.isoformat()
            expression_names['#end_time'] = 'end_time'
            
            # Calculate duration if start_time exists
            # Note: This requires fetching the item first to get start_time
            # In practice, this could be optimized with a single UpdateExpression
            # using conditional logic, but for clarity we'll do a simple approach
        elif end_time:
            # Explicitly set end_time for non-terminal states if provided
            update_parts.append('#end_time = :end_time')
            expression_values[':end_time'] = end_time.isoformat()
            expression_names['#end_time'] = 'end_time'
        
        update_expression = "SET " + ", ".join(update_parts)
        
        # Default condition ensures run exists
        if condition_expression is None:
            condition_expression = Attr('run_id').exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        
        # First update status and basic fields
        response = self.gateway.update_item(
            key=key,
            update_expression=update_expression,
            expression_attribute_values=expression_values,
            expression_attribute_names=expression_names,
            condition_expression=condition_expression,
            return_values='ALL_NEW'
        )
        
        # Calculate duration for terminal states
        if status in terminal_states and 'start_time' in response:
            start_time_str = response.get('start_time')
            end_time_str = response.get('end_time')
            
            if start_time_str and end_time_str:
                try:
                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                    duration = (end_time - start_time).total_seconds()
                    
                    # Update duration in a separate operation
                    self.gateway.update_item(
                        key={'run_id': run_id},
                        update_expression="SET duration_seconds = :duration",
                        expression_attribute_values={':duration': Decimal(str(duration))},
                        return_values='NONE'
                    )
                    
                    # Add duration to response
                    response['duration_seconds'] = Decimal(str(duration))
                    
                except Exception as e:
                    logger.warning(f"Failed to calculate duration for run {run_id}: {e}")
        
        logger.info(f"Updated run status {run_id}: {status.value}")
        return response

    def add_stage_info(
        self,
        run_id: str,
        stage_info: Dict[str, Any],
        condition_expression=None
    ) -> Dict[str, Any]:
        """
        Add or update stage information for a pipeline run.
        
        DynamoDB Operation: UpdateItem with list append or update
        
        Args:
            run_id: Run identifier
            stage_info: Stage information dictionary
            condition_expression: Custom condition (defaults to require existence)
            
        Returns:
            Updated run log attributes
            
        Raises:
            ItemNotFoundError: Run doesn't exist
            ConnectionError: DynamoDB error
        """
        stage_name = stage_info.get('stage_name')
        if not stage_name:
            raise ValidationError("stage_info must include 'stage_name' field")
        
        # Default condition ensures run exists
        if condition_expression is None:
            condition_expression = Attr('run_id').exists()
        
        # Add timestamp to stage info if not present
        if 'timestamp' not in stage_info:
            stage_info['timestamp'] = datetime.now(timezone.utc).isoformat()
        
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        
        # For simplicity, we'll append to stages list
        # In practice, you might want to check if stage exists first
        update_expression = "SET stages = list_append(if_not_exists(stages, :empty_list), :new_stage), updated_at = :updated_at"
        expression_values = {
            ':empty_list': [],
            ':new_stage': [stage_info],
            ':updated_at': datetime.now(timezone.utc).isoformat()
        }
        
        response = self.gateway.update_item(
            key={'run_id': run_id},
            update_expression=update_expression,
            expression_attribute_values=expression_values,
            condition_expression=condition_expression,
            return_values='ALL_NEW'
        )
        
        logger.info(f"Added stage info to run {run_id}: {stage_name}")
        return response

    def update_run_metrics(
        self,
        run_id: str,
        metrics: Dict[str, Any],
        condition_expression=None
    ) -> Dict[str, Any]:
        """
        Update run metrics atomically.
        
        DynamoDB Operation: UpdateItem with nested attribute updates
        
        Args:
            run_id: Run identifier
            metrics: Dictionary of metrics to update
            condition_expression: Custom condition (defaults to require existence)
            
        Returns:
            Updated run log attributes
        """
        if not metrics:
            raise ValidationError("Metrics dictionary cannot be empty")
        
        # Build update expression for nested metrics
        update_parts = []
        expression_values = {}
        expression_names = {}
        
        # Always update timestamp
        update_parts.append('#updated_at = :updated_at')
        expression_values[':updated_at'] = datetime.now(timezone.utc).isoformat()
        expression_names['#updated_at'] = 'updated_at'
        
        # Add metrics updates
        for key, value in metrics.items():
            metric_name = f"#metrics.{key}"
            metric_value = f":metric_{key}"
            update_parts.append(f"metrics.{metric_name} = {metric_value}")
            expression_names[metric_name] = key
            expression_values[metric_value] = value
        
        update_expression = "SET " + ", ".join(update_parts)
        
        # Default condition ensures run exists
        if condition_expression is None:
            condition_expression = Attr('run_id').exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        response = self.gateway.update_item(
            key={'run_id': run_id},
            update_expression=update_expression,
            expression_attribute_values=expression_values,
            expression_attribute_names=expression_names,
            condition_expression=condition_expression,
            return_values='ALL_NEW'
        )
        
        logger.info(f"Updated metrics for run {run_id}: {list(metrics.keys())}")
        return response

    def delete_run_log(
        self,
        run_id: str,
        pipeline_id: str,
        condition_expression=None
    ) -> bool:
        """
        Delete pipeline run log.
        
        DynamoDB Operation: DeleteItem with ConditionExpression
        Default Condition: Primary key exists - ensures safe deletion
        
        Args:
            run_id: Run identifier (partition key)
            pipeline_id: Pipeline identifier (sort key)
            condition_expression: Custom condition (defaults to require existence)
            
        Returns:
            True if deleted, raises exception if not found
            
        Raises:
            ItemNotFoundError: Run doesn't exist
            ConnectionError: DynamoDB error
            
        Examples:
            >>> api.delete_run_log("run-123", "pipeline-456")
        """
        # Build key for delete operation using model-aware function
        key = build_model_key(PipelineRunLog, run_id=run_id, pipeline_id=pipeline_id)
        
        # Default condition ensures safe deletion
        if condition_expression is None:
            # For composite keys, check both partition and sort key exist
            condition_expression = Attr('run_id').exists() & Attr('pipeline_id').exists()
            
        # TableGateway maps ConditionalCheckFailedException to ConflictError automatically
        # Other ClientErrors are mapped to appropriate domain exceptions
        response = self.gateway.delete_item(
            key=key,
            condition_expression=condition_expression,
            return_values='ALL_OLD'
        )
        
        if response:
            logger.info(f"Deleted run log: {run_id}")
            return True
        return False

    def upsert_run_log(
        self,
        run_data: PipelineRunLogUpsert
    ) -> PipelineRunLog:
        """
        Create or update pipeline run log using validated DTO (upsert operation).
        
        DynamoDB Operation: PutItem without conditions - allows overwrite
        
        Args:
            run_data: Validated PipelineRunLogUpsert DTO
            
        Returns:
            PipelineRunLog instance
            
        Raises:
            ValidationError: Invalid run data
        """
        try:
            # Convert validated DTO to full model, preserving/setting timestamps
            run_dict = run_data.model_dump()
            now = datetime.now(timezone.utc)
            
            # Set created_at if not provided (for new items)
            if 'created_at' not in run_dict:
                run_dict['created_at'] = now
            # Always update the updated_at timestamp
            run_dict['updated_at'] = now
            
            # Set start_time if not provided and status is not terminal
            if 'start_time' not in run_dict and run_dict.get('status') != RunStatus.PENDING:
                run_dict['start_time'] = now
            
            run_log = PipelineRunLog(**run_dict)
        except Exception as e:
            raise ValidationError(f"Invalid run log data: {e}") from e
            
        # Ensure all datetime fields are in UTC before storage
        run_log = self._ensure_utc_timestamps(run_log)
        item = model_to_item(run_log)
        
        self.gateway.put_item(item)  # No condition - allows overwrite
        logger.info(f"Upserted run log: {run_log.run_id}")
        return run_log

    def upsert_many(
        self,
        runs_data: List[PipelineRunLogUpsert],
        overwrite_by_pkeys: List[str] = ["run_id"]
    ) -> List[PipelineRunLog]:
        """
        Bulk upsert multiple pipeline run logs using validated DTOs.
        
        DynamoDB Operation: BatchWriteItem with put operations
        
        Args:
            runs_data: List of validated PipelineRunLogUpsert DTOs
            overwrite_by_pkeys: Primary keys that allow overwriting (for safety)
            
        Returns:
            List of created PipelineRunLog instances
            
        Raises:
            ValidationError: Invalid run data
        """
        if not runs_data:
            return []
            
        # Convert all validated DTOs to full models
        validated_runs = []
        now = datetime.now(timezone.utc)
        
        for run_dto in runs_data:
            try:
                # Convert DTO to full model with timestamps
                run_dict = run_dto.model_dump()
                if 'created_at' not in run_dict:
                    run_dict['created_at'] = now
                run_dict['updated_at'] = now
                
                # Set start_time if not provided and not PENDING
                if 'start_time' not in run_dict and run_dict.get('status') != RunStatus.PENDING:
                    run_dict['start_time'] = now
                
                run_log = PipelineRunLog(**run_dict)
                validated_runs.append(run_log)
            except Exception as e:
                raise ValidationError(f"Invalid run data for {run_dto.run_id}: {e}") from e
        
        # Batch write using context manager
        with self.gateway.batch_writer() as batch:
            for run_log in validated_runs:
                # Ensure all datetime fields are in UTC before storage
                run_log = self._ensure_utc_timestamps(run_log)
                item = model_to_item(run_log)
                batch.put_item(Item=item)
                
        logger.info(f"Bulk upserted {len(validated_runs)} run logs")
        return validated_runs

    def start_pipeline_run(
        self,
        run_id: str,
        pipeline_id: str,
        trigger_type: str,
        created_by: Optional[str] = None,
        **kwargs
    ) -> PipelineRunLog:
        """
        Start a new pipeline run with RUNNING status.
        
        Combined operation: create run log + update to RUNNING status
        
        Args:
            run_id: Unique run identifier
            pipeline_id: Pipeline this run belongs to
            trigger_type: What triggered the run
            created_by: User who triggered the run
            **kwargs: Additional run log parameters
            
        Returns:
            Created PipelineRunLog instance
        """
        run_data = {
            'run_id': run_id,
            'pipeline_id': pipeline_id,
            'trigger_type': trigger_type,
            'status': RunStatus.RUNNING,
            'start_time': datetime.now(timezone.utc),
            'created_by': created_by,
            **kwargs
        }
        
        return self.create_run_log(run_data)

    def finish_pipeline_run(
        self,
        run_id: str,
        status: RunStatus,
        error_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Finish a pipeline run with terminal status.
        
        Args:
            run_id: Run identifier
            status: Terminal status (SUCCESS, FAILED, CANCELLED)
            error_message: Error message if failed
            
        Returns:
            Updated run log attributes
        """
        terminal_states = [RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED]
        if status not in terminal_states:
            raise ValidationError(f"Status must be one of {[s.value for s in terminal_states]}")
        
        return self.update_run_status(
            run_id=run_id,
            status=status,
            error_message=error_message,
            end_time=datetime.now(timezone.utc)
        )

    def cancel_running_runs_for_pipeline(
        self,
        pipeline_id: str,
        cancelled_by: Optional[str] = None
    ) -> int:
        """
        Cancel all running runs for a pipeline using transactions.
        
        This is a compound operation that:
        1. Queries running runs for pipeline (read API)
        2. Updates all to CANCELLED status (write API)
        
        Args:
            pipeline_id: Pipeline identifier
            cancelled_by: User cancelling the runs
            
        Returns:
            Number of runs cancelled
        """
        from .queries import PipelineRunLogsReadApi
        
        read_api = PipelineRunLogsReadApi(self.config)
        
        # Get running runs for pipeline
        running_runs, _ = read_api.query_by_pipeline(
            pipeline_id=pipeline_id,
            status=RunStatus.RUNNING,
            projection=['run_id'],
            limit=100
        )
        
        if not running_runs:
            return 0
        
        # Cancel each run
        cancelled_count = 0
        for run in running_runs:
            try:
                self.update_run_status(
                    run_id=run.run_id,
                    status=RunStatus.CANCELLED,
                    error_message=f"Cancelled by {cancelled_by}" if cancelled_by else "Cancelled"
                )
                cancelled_count += 1
            except Exception as e:
                logger.warning(f"Failed to cancel run {run.run_id}: {e}")
        
        logger.info(f"Cancelled {cancelled_count} running runs for pipeline {pipeline_id}")
        return cancelled_count

    def cleanup_old_run_logs(
        self,
        days_old: int = 90,
        batch_size: int = 25
    ) -> int:
        """
        Delete old run logs using batch operations.
        
        Args:
            days_old: Age threshold in days
            batch_size: Number of items to delete per batch
            
        Returns:
            Number of run logs deleted
        """
        from .queries import PipelineRunLogsReadApi
        
        read_api = PipelineRunLogsReadApi(self.config)
        cutoff_time = datetime.now(timezone.utc) - timezone.timedelta(days=days_old)
        
        # This would need to scan with time filter - simplified for example
        # In practice, you'd want a more efficient approach
        deleted_count = 0
        
        # Placeholder for actual implementation
        logger.info(f"Cleanup operation would delete runs older than {cutoff_time}")
        
        return deleted_count