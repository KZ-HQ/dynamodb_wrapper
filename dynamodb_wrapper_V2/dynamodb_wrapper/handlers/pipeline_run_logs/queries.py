"""
Pipeline Run Logs Read API

This module provides optimized read operations for pipeline run logs using:
- GSI queries for pipeline and status filtering with time-based sorting
- Projection expressions for minimal data transfer
- Time range queries with efficient KeyConditionExpression
- Pagination support with last_key tokens

All methods return (items, last_key) tuples for proper pagination handling.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Tuple

from boto3.dynamodb.conditions import Key, Attr

from ...config import DynamoDBConfig
from ...models import PipelineRunLog, RunStatus, PipelineRunLogView, PipelineRunLogSummaryView
from ...utils import (
    item_to_model, build_projection_expression, 
    to_user_timezone,
    build_model_key, build_model_key_condition, build_gsi_key_condition
)
from ...core import create_table_gateway

logger = logging.getLogger(__name__)


class PipelineRunLogsReadApi:
    """
    Read-only API for pipeline run log queries.
    
    Uses optimized DynamoDB access patterns:
    - GSI queries for pipeline and status filtering with time ordering
    - Range queries for time-based filtering
    - Projection expressions to minimize data transfer
    - Proper pagination with ExclusiveStartKey
    """

    def __init__(self, config: DynamoDBConfig):
        """Initialize read API with configuration."""
        self.config = config
        self.gateway = create_table_gateway(config, "pipeline_run_logs")
        
        # Default minimal projection for list operations
        self.default_projection = [
            'run_id', 'pipeline_id', 'status', 'trigger_type',
            'start_time', 'end_time', 'duration_seconds', 
            'created_by', 'created_at', 'updated_at'
        ]
    
    def _convert_to_user_timezone(self, model_instance):
        """Convert UTC datetimes in model to user's configured timezone.
        
        This method handles timezone conversion at the handler boundary.
        Gateway layer provides UTC data, handlers convert for user presentation.
        
        Args:
            model_instance: Model instance with UTC datetimes
            
        Returns:
            Model instance with datetimes in user's configured timezone
        """
        user_timezone = self.config.user_timezone
        
        # If no user timezone configured, return as-is (UTC)
        if not user_timezone:
            return model_instance
            
        model_dict = model_instance.model_dump()
        
        # List of datetime fields that need conversion
        datetime_fields = ['start_time', 'end_time', 'created_at', 'updated_at']
        
        for field in datetime_fields:
            if field in model_dict and model_dict[field] is not None:
                dt = model_dict[field]
                if isinstance(dt, datetime):
                    model_dict[field] = to_user_timezone(dt, user_timezone)
        
        # Handle datetime fields in stages if present
        if 'stages' in model_dict and model_dict['stages']:
            for stage in model_dict['stages']:
                for field in ['start_time', 'end_time']:
                    if field in stage and stage[field] is not None:
                        dt = stage[field]
                        if isinstance(dt, datetime):
                            stage[field] = to_user_timezone(dt, user_timezone)
        
        return type(model_instance)(**model_dict)

    def get_by_id(
        self,
        run_id: str,
        pipeline_id: str,
        projection: Optional[List[str]] = None
    ) -> Optional[PipelineRunLogView]:
        """
        Get pipeline run log by ID (optimized read model).
        
        DynamoDB Operation: GetItem with primary key
        
        Args:
            run_id: Run identifier (partition key)
            pipeline_id: Pipeline identifier (sort key)
            projection: Fields to return (uses view projection if None).
                       Projections reduce RCU consumption and latency by limiting returned data.            
        Returns:
            PipelineRunLogView if found, None otherwise
            
        Examples:
            >>> run = api.get_by_id("run-123", "pipeline-456")
            >>> run = api.get_by_id("run-123", "pipeline-456", projection=['run_id', 'status'])
        """
        # Build key for get operation using model-aware function
        key = build_model_key(PipelineRunLog, run_id=run_id, pipeline_id=pipeline_id)
        
        get_kwargs = {
            'Key': key
        }
        
        # Use view projection if none specified (optimized for read operations)
        if projection:
            proj_expr, expr_names = build_projection_expression(projection)
        else:
            # Use optimized projection for PipelineRunLogView
            view_projection = [
                'run_id', 'pipeline_id', 'status', 'trigger_type',
                'start_time', 'end_time', 'duration_seconds',
                'total_records_processed', 'total_records_failed',
                'error_message', 'retry_count', 'environment', 'pipeline_version',
                'data_quality_passed', 'created_by', 'tags', 'created_at', 'updated_at'
            ]
            proj_expr, expr_names = build_projection_expression(view_projection)
            
        if proj_expr:
            get_kwargs['ProjectionExpression'] = proj_expr
            get_kwargs['ExpressionAttributeNames'] = expr_names
            
        response = self.gateway.table.get_item(**get_kwargs)
        
        if 'Item' not in response:
            return None
            
        return self._convert_to_user_timezone(item_to_model(response['Item'], PipelineRunLogView))

    def query_by_pipeline(
        self,
        pipeline_id: str,
        status: Optional[RunStatus] = None,
        start_time_range: Optional[Tuple[datetime, datetime]] = None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineRunLogView], Optional[dict]]:
        """
        Query pipeline run logs by pipeline with time-ordered results.
        
        DynamoDB Operation: Query on PipelineRunsIndex GSI
        GSI Structure: PK=pipeline_id, SK=start_time (for chronological ordering)
        
        Args:
            pipeline_id: Pipeline identifier
            status: Optional status filter (uses FilterExpression)
            start_time_range: Optional time range filter (from_time, to_time)
            projection: Fields to return (uses default minimal projection if None).
                       Projections reduce RCU consumption and latency by limiting returned data.
            limit: Maximum items to return
            last_key: Pagination token from previous query            
        Returns:
            Tuple of (run_log_list, next_page_token)
        """
        query_kwargs = {
            'IndexName': 'PipelineRunsIndex',
            'KeyConditionExpression': build_gsi_key_condition(PipelineRunLog, 'PipelineRunsIndex', pipeline_id=pipeline_id),
            'ScanIndexForward': False  # Most recent first
        }
        
        # Handle projection safely - reduces RCUs and latency for log queries
        proj_expr, expr_names = build_projection_expression(projection or self.default_projection)
        if proj_expr:
            query_kwargs['ProjectionExpression'] = proj_expr
            query_kwargs['ExpressionAttributeNames'] = expr_names
        
        # Add time range filter to KeyCondition if specified
        if start_time_range:
            from_time, to_time = start_time_range
            if from_time and to_time:
                query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & \
                    Key('start_time').between(from_time.isoformat(), to_time.isoformat())
            elif from_time:
                query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & \
                    Key('start_time').gte(from_time.isoformat())
            elif to_time:
                query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & \
                    Key('start_time').lte(to_time.isoformat())
        
        # Add status filter if specified
        if status:
            query_kwargs['FilterExpression'] = Attr('status').eq(status.value)
            
        if limit:
            query_kwargs['Limit'] = limit
        if last_key:
            query_kwargs['ExclusiveStartKey'] = last_key
            
        try:
            response = self.gateway.query(**query_kwargs)
            
            items = [
                self._convert_to_user_timezone(item_to_model(item, PipelineRunLogView))
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in query_by_pipeline: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to query pipeline run logs by pipeline: {e}", e) from e

    def query_by_status(
        self,
        status: RunStatus,
        pipeline_id: Optional[str] = None,
        time_range: Optional[Tuple[datetime, datetime]] = None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineRunLogView], Optional[dict]]:
        """
        Query pipeline run logs by status with time-ordered results.
        
        DynamoDB Operation: Query on StatusRunsIndex GSI
        GSI Structure: PK=status, SK=start_time (for chronological ordering)
        
        Args:
            status: Run status to filter by
            pipeline_id: Optional pipeline ID filter (uses FilterExpression)
            time_range: Optional time range filter (from_time, to_time)
            projection: Fields to return (uses default minimal projection if None).
                       Projections reduce RCU consumption and latency by limiting returned data.
            limit: Maximum items to return
            last_key: Pagination token from previous query            
        Returns:
            Tuple of (run_log_list, next_page_token)
        """
        query_kwargs = {
            'IndexName': 'StatusRunsIndex',
            'KeyConditionExpression': build_gsi_key_condition(PipelineRunLog, 'StatusRunsIndex', status=status.value),
            'ScanIndexForward': False  # Most recent first
        }
        
        # Handle projection safely - important for status queries
        proj_expr, expr_names = build_projection_expression(projection or self.default_projection)
        if proj_expr:
            query_kwargs['ProjectionExpression'] = proj_expr
            query_kwargs['ExpressionAttributeNames'] = expr_names
        
        # Add time range filter to KeyCondition if specified
        if time_range:
            from_time, to_time = time_range
            if from_time and to_time:
                query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & \
                    Key('start_time').between(from_time.isoformat(), to_time.isoformat())
            elif from_time:
                query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & \
                    Key('start_time').gte(from_time.isoformat())
            elif to_time:
                query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & \
                    Key('start_time').lte(to_time.isoformat())
        
        # Add pipeline filter if specified
        if pipeline_id:
            query_kwargs['FilterExpression'] = Attr('pipeline_id').eq(pipeline_id)
            
        if limit:
            query_kwargs['Limit'] = limit
        if last_key:
            query_kwargs['ExclusiveStartKey'] = last_key
            
        try:
            response = self.gateway.query(**query_kwargs)
            
            items = [
                self._convert_to_user_timezone(item_to_model(item, PipelineRunLogView))
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in query_by_status: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to query pipeline run logs by status: {e}", e) from e

    def get_running_pipelines(
        self,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineRunLogView], Optional[dict]]:
        """
        Get all currently running pipeline runs.
        
        DynamoDB Operation: Query on StatusRunsIndex GSI for RUNNING status
        
        Args:
            projection: Fields to return
            limit: Maximum items to return
            last_key: Pagination token            
        Returns:
            Tuple of (running_run_list, next_page_token)
        """
        return self.query_by_status(
            status=RunStatus.RUNNING,
            projection=projection,
            limit=limit,
            last_key=last_key)

    def get_failed_runs(
        self,
        pipeline_id: Optional[str] = None,
        hours: int = 24,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineRunLogView], Optional[dict]]:
        """
        Get failed pipeline runs within specified time window.
        
        DynamoDB Operation: Query on StatusRunsIndex GSI with time range
        
        Args:
            pipeline_id: Optional pipeline ID filter
            hours: Number of hours to look back (default: 24)
            projection: Fields to return
            limit: Maximum items to return
            last_key: Pagination token            
        Returns:
            Tuple of (failed_run_list, next_page_token)
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        return self.query_by_status(
            status=RunStatus.FAILED,
            pipeline_id=pipeline_id,
            time_range=(cutoff_time, None),
            projection=projection,
            limit=limit,
            last_key=last_key)

    def get_recent_runs(
        self,
        pipeline_id: str,
        hours: int = 24,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineRunLogView], Optional[dict]]:
        """
        Get recent pipeline runs within specified time window.
        
        DynamoDB Operation: Query on PipelineRunsIndex GSI with time range
        
        Args:
            pipeline_id: Pipeline identifier
            hours: Number of hours to look back (default: 24)
            projection: Fields to return
            limit: Maximum items to return
            last_key: Pagination token            
        Returns:
            Tuple of (recent_run_list, next_page_token)
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        return self.query_by_pipeline(
            pipeline_id=pipeline_id,
            start_time_range=(cutoff_time, None),
            projection=projection,
            limit=limit,
            last_key=last_key)

    def get_successful_runs(
        self,
        pipeline_id: Optional[str] = None,
        days: int = 7,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineRunLogView], Optional[dict]]:
        """
        Get successful pipeline runs within specified time window.
        
        DynamoDB Operation: Query on StatusRunsIndex GSI with time range
        
        Args:
            pipeline_id: Optional pipeline ID filter
            days: Number of days to look back (default: 7)
            projection: Fields to return
            limit: Maximum items to return
            last_key: Pagination token            
        Returns:
            Tuple of (successful_run_list, next_page_token)
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
        
        return self.query_by_status(
            status=RunStatus.SUCCESS,
            pipeline_id=pipeline_id,
            time_range=(cutoff_time, None),
            projection=projection,
            limit=limit,
            last_key=last_key)

    def get_run_summary(
        self,
        run_id: str
    ) -> Optional[PipelineRunLogSummaryView]:
        """
        Get minimal run summary for dashboard/list views.
        
        DynamoDB Operation: GetItem with minimal projection
        
        Args:
            run_id: Run identifier            
        Returns:
            PipelineRunLogSummaryView if found, None otherwise
        """
        summary_projection = [
            'run_id', 'pipeline_id', 'status', 'start_time', 
            'duration_seconds', 'error_message'
        ]
        
        proj_expr, expr_names = build_projection_expression(summary_projection)
        get_kwargs = {
            'Key': {'run_id': run_id}
        }
        
        if proj_expr:
            get_kwargs['ProjectionExpression'] = proj_expr
            get_kwargs['ExpressionAttributeNames'] = expr_names
        
        response = self.gateway.table.get_item(**get_kwargs)
        
        if 'Item' not in response:
            return None
            
        return self._convert_to_user_timezone(item_to_model(response['Item'], PipelineRunLogSummaryView))

    def scan_for_all_runs(
        self,
        projection: Optional[List[str]] = None,
        limit: int = 100,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineRunLogView], Optional[dict]]:
        """
        Scan all pipeline run logs (DISCOURAGED - use queries when possible).
        
        DynamoDB Operation: Scan with required projection and limit
        
        ⚠️ WARNING: This is an expensive operation. Consider using:
        - query_by_pipeline() for pipeline-specific queries
        - query_by_status() for status-specific queries
        
        Args:
            projection: REQUIRED - Fields to return to limit data transfer.
                       Projections are critical for scan operations to reduce RCUs and costs.
            limit: REQUIRED - Maximum items per page (default: 100)
            last_key: Pagination token from previous scan            
        Returns:
            Tuple of (run_log_list, next_page_token)
        """
        if not projection:
            projection = self.default_projection
            logger.warning("scan_for_all_runs: Using default projection to prevent excessive data transfer")
        
        scan_kwargs = {
            'Limit': limit
        }
        
        # Handle projection safely - critical for scan operations on log data
        proj_expr, expr_names = build_projection_expression(projection)
        if proj_expr:
            scan_kwargs['ProjectionExpression'] = proj_expr
            scan_kwargs['ExpressionAttributeNames'] = expr_names
        
        if last_key:
            scan_kwargs['ExclusiveStartKey'] = last_key
            
        try:
            response = self.gateway.scan(**scan_kwargs)
            
            items = [
                self._convert_to_user_timezone(item_to_model(item, PipelineRunLogView))
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in scan_for_all_runs: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to scan all pipeline run logs: {e}", e) from e

    def count_runs_by_pipeline(self, pipeline_id: str) -> int:
        """
        Count runs for a specific pipeline.
        
        DynamoDB Operation: Query on PipelineRunsIndex GSI with Select=COUNT
        
        Args:
            pipeline_id: Pipeline to count runs for
            
        Returns:
            Number of runs for pipeline
        """
        try:
            response = self.gateway.query(
                IndexName='PipelineRunsIndex',
                KeyConditionExpression=build_gsi_key_condition(PipelineRunLog, 'PipelineRunsIndex', pipeline_id=pipeline_id),
                Select='COUNT'
            )
            
            return response.get('Count', 0)
        except Exception as e:
            logger.error(f"Error in count_runs_by_pipeline: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to count runs by pipeline: {e}", e) from e

    def count_runs_by_status(self, status: RunStatus) -> int:
        """
        Count runs with specific status.
        
        DynamoDB Operation: Query on StatusRunsIndex GSI with Select=COUNT
        
        Args:
            status: Status to count
            
        Returns:
            Number of runs with specified status
        """
        try:
            response = self.gateway.query(
                IndexName='StatusRunsIndex',
                KeyConditionExpression=build_gsi_key_condition(PipelineRunLog, 'StatusRunsIndex', status=status.value),
                Select='COUNT'
            )
            
            return response.get('Count', 0)
        except Exception as e:
            logger.error(f"Error in count_runs_by_status: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to count runs by status: {e}", e) from e

    def get_pipeline_run_statistics(
        self,
        pipeline_id: str,
        days: int = 30
    ) -> dict:
        """
        Get aggregated run statistics for a pipeline.
        
        Args:
            pipeline_id: Pipeline identifier
            days: Number of days to analyze
            
        Returns:
            Dictionary with aggregated statistics
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
        
        # Get recent runs with minimal data for statistics
        runs, _ = self.query_by_pipeline(
            pipeline_id,
            start_time_range=(cutoff_time, None),
            projection=['status', 'duration_seconds', 'start_time'],
            limit=1000  # Reasonable limit for statistics
        )
        
        # Calculate statistics
        stats = {
            'total_runs': len(runs),
            'success_rate': 0,
            'avg_duration_seconds': 0,
            'status_counts': {},
            'runs_by_day': {}
        }
        
        if not runs:
            return stats
        
        # Aggregate by status
        total_duration = 0
        successful_runs = 0
        
        for run in runs:
            status = run.status.value if run.status else 'unknown'
            stats['status_counts'][status] = stats['status_counts'].get(status, 0) + 1
            
            if run.status == RunStatus.SUCCESS:
                successful_runs += 1
                
            if run.duration_seconds:
                total_duration += float(run.duration_seconds)
                
            # Group by day
            if run.start_time:
                day_key = run.start_time.date().isoformat()
                stats['runs_by_day'][day_key] = stats['runs_by_day'].get(day_key, 0) + 1
        
        # Calculate derived metrics
        stats['success_rate'] = (successful_runs / len(runs)) * 100 if runs else 0
        stats['avg_duration_seconds'] = total_duration / len(runs) if runs else 0
        
        return stats

    def query_by_composite_key_range(
        self,
        partition_key: str,
        partition_value: Any,
        sort_key: str,
        sort_condition: str,
        sort_value: Any,
        sort_value2: Optional[Any] = None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None,
        ascending: bool = False
    ) -> Tuple[List[PipelineRunLogView], Optional[dict]]:
        """
        Query using composite key with flexible sort key conditions.
        
        This is the most powerful query method that supports various composite key patterns:
        - Time range queries (between)
        - Prefix searches (begins_with)
        - Exact matches (eq)
        - Comparison queries (gt, lt, gte, lte)
        
        DynamoDB Operation: Query with KeyConditionExpression on composite key
        
        Args:
            partition_key: Partition key attribute name
            partition_value: Partition key value
            sort_key: Sort key attribute name
            sort_condition: Sort key condition ('eq', 'begins_with', 'between', 'gt', 'lt', 'gte', 'lte')
            sort_value: Sort key value (or lower bound for 'between')
            sort_value2: Upper bound for 'between' condition
            projection: Fields to return (uses default if None)
            limit: Maximum items to return
            last_key: Pagination token
            ascending: Sort order (False = newest first for timestamps)
            
        Returns:
            Tuple of (run_logs, next_pagination_token)
            
        Examples:
            # Time range query with composite key
            runs, next_key = api.query_by_composite_key_range(
                partition_key="pipeline_id",
                partition_value="pipeline-123",
                sort_key="run_timestamp",
                sort_condition="between",
                sort_value="2024-01-01T00:00:00Z",
                sort_value2="2024-01-31T23:59:59Z"
            )
            
            # Date prefix query
            runs, next_key = api.query_by_composite_key_range(
                partition_key="pipeline_id", 
                partition_value="pipeline-123",
                sort_key="run_timestamp",
                sort_condition="begins_with",
                sort_value="2024-01-15"
            )
            
            # Recent runs (greater than)
            runs, next_key = api.query_by_composite_key_range(
                partition_key="pipeline_id",
                partition_value="pipeline-123", 
                sort_key="run_timestamp",
                sort_condition="gt",
                sort_value="2024-01-01T00:00:00Z"
            )
        """
        logger.info(f"Composite key query: {partition_key}={partition_value}, {sort_key} {sort_condition} {sort_value}")
        
        # Build key condition using the utility
        key_condition = build_key_condition(
            partition_key=partition_key,
            partition_value=partition_value,
            sort_key=sort_key,
            sort_condition=sort_condition,
            sort_value=sort_value,
            sort_value2=sort_value2
        )
        
        query_kwargs = {
            'KeyConditionExpression': key_condition,
            'ScanIndexForward': ascending
        }
        
        # Handle projection
        proj_expr, expr_names = build_projection_expression(projection or self.default_projection)
        if proj_expr:
            query_kwargs['ProjectionExpression'] = proj_expr
            query_kwargs['ExpressionAttributeNames'] = expr_names
        
        if limit:
            query_kwargs['Limit'] = limit
        if last_key:
            query_kwargs['ExclusiveStartKey'] = last_key
            
        try:
            response = self.gateway.query(**query_kwargs)
            
            items = [
                self._convert_to_user_timezone(item_to_model(item, PipelineRunLogView))
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in query_by_composite_key_range: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to query by composite key range: {e}", e) from e

    def query_time_series(
        self,
        partition_key: str,
        partition_value: Any,
        time_sort_key: str,
        start_time: datetime,
        end_time: datetime,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineRunLogView], Optional[dict]]:
        """
        Convenience method for time-series queries using composite keys.
        
        This is a specialized version of query_by_composite_key_range for time-based data.
        
        Args:
            partition_key: Partition key attribute name
            partition_value: Partition key value  
            time_sort_key: Sort key attribute name containing timestamps
            start_time: Start of time range (inclusive)
            end_time: End of time range (inclusive)
            projection: Fields to return (uses default if None)
            limit: Maximum items to return
            last_key: Pagination token
            
        Returns:
            Tuple of (run_logs, next_pagination_token)
            
        Example:
            # Get runs from last 24 hours using composite key
            now = datetime.now(timezone.utc)
            yesterday = now - timedelta(days=1)
            
            runs, next_key = api.query_time_series(
                partition_key="pipeline_id",
                partition_value="pipeline-123",
                time_sort_key="run_timestamp", 
                start_time=yesterday,
                end_time=now
            )
        """
        return self.query_by_composite_key_range(
            partition_key=partition_key,
            partition_value=partition_value,
            sort_key=time_sort_key,
            sort_condition="between",
            sort_value=start_time.isoformat(),
            sort_value2=end_time.isoformat(),
            projection=projection,
            limit=limit,
            last_key=last_key,
            ascending=False  # Newest first for time series
        )
