"""
Pipeline Configuration Read API

This module provides optimized read operations for pipeline configurations using:
- DynamoDB Query operations with GSIs for efficient filtering
- Pagination support with last_key tokens
- ProjectionExpression for minimal data transfer
- No inefficient full table scans

All methods return (items, last_key) tuples for proper pagination handling.
"""

import logging
from datetime import datetime
from typing import List, Optional, Tuple

from boto3.dynamodb.conditions import Key, Attr

from ...config import DynamoDBConfig
from ...models import PipelineConfig, PipelineConfigView, PipelineConfigSummaryView
from ...utils import item_to_model, build_projection_expression, build_filter_expression
from ...core import create_table_gateway

logger = logging.getLogger(__name__)


class PipelineConfigReadApi:
    """
    Read-only API for pipeline configuration queries.
    
    Uses optimized DynamoDB access patterns:
    - Primary key lookups with get_item
    - GSI queries for environment and status filtering
    - Projection expressions to minimize data transfer
    - Proper pagination with ExclusiveStartKey
    """

    def __init__(self, config: DynamoDBConfig):
        """Initialize read API with configuration."""
        self.config = config
        self.gateway = create_table_gateway(config, "pipeline_config")
        
        # Default minimal projection for list operations
        self.default_projection = [
            'pipeline_id', 'pipeline_name', 'source_type', 'destination_type',
            'is_active', 'environment', 'created_at', 'updated_at'
        ]

    def get_by_id(
        self, 
        pipeline_id: str,
        projection: Optional[List[str]] = None
    ) -> Optional[PipelineConfigView]:
        """
        Get pipeline configuration by ID (optimized read model).
        
        DynamoDB Operation: GetItem with primary key
        
        Args:
            pipeline_id: Pipeline identifier
            projection: Fields to return (uses view projection if None). 
                       Projections reduce RCU consumption and latency by limiting returned data.
            
        Returns:
            PipelineConfigView if found, None otherwise
        """
        get_kwargs = {
            'Key': {'pipeline_id': pipeline_id}
        }
        
        # Use view projection if none specified (optimized for read operations)
        if projection:
            proj_expr, expr_names = build_projection_expression(projection)
        else:
            # Use optimized projection for PipelineConfigView
            view_projection = [
                'pipeline_id', 'pipeline_name', 'description', 'source_type', 
                'destination_type', 'is_active', 'schedule_expression', 
                'environment', 'version', 'tags', 'created_at', 'updated_at', 
                'created_by', 'updated_by'
            ]
            proj_expr, expr_names = build_projection_expression(view_projection)
            
        if proj_expr:
            get_kwargs['ProjectionExpression'] = proj_expr
            get_kwargs['ExpressionAttributeNames'] = expr_names
            
        try:
            response = self.gateway.table.get_item(**get_kwargs)
            
            if 'Item' not in response:
                return None
                
            return item_to_model(response['Item'], PipelineConfigView)
        except Exception as e:
            # TableGateway already maps ClientError to domain exceptions,
            # but we catch any other potential errors here
            logger.error(f"Error in get_by_id: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to get pipeline {pipeline_id}: {e}", e) from e

    def query_active_pipelines(
        self,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineConfigView], Optional[dict]]:
        """
        Query active pipeline configurations.
        
        DynamoDB Operation: Query on ActivePipelinesIndex GSI
        GSI Structure: PK=is_active, SK=updated_at (for recency ordering)
        
        Args:
            projection: Fields to return (uses default minimal projection if None).
                       Projections reduce RCU consumption and latency by limiting returned data.
            limit: Maximum items to return
            last_key: Pagination token from previous query
            
        Returns:
            Tuple of (pipeline_list, next_page_token)
        """
        query_kwargs = {
            'IndexName': 'ActivePipelinesIndex',
            'KeyConditionExpression': Key('is_active').eq('true'),
            'ScanIndexForward': False  # Most recently updated first
        }
        
        # Handle projection safely with reserved word protection
        # Note: Projections reduce RCU consumption and latency
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
                item_to_model(item, PipelineConfigView)
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in query_active_pipelines: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to query active pipelines: {e}", e) from e

    def query_by_environment(
        self,
        environment: str,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineConfigView], Optional[dict]]:
        """
        Query pipeline configurations by environment.
        
        DynamoDB Operation: Query on EnvironmentIndex GSI
        GSI Structure: PK=environment, SK=created_at (for chronological ordering)
        
        Args:
            environment: Environment to filter by (dev, staging, prod)
            projection: Fields to return (uses default minimal projection if None).
                       Projections reduce RCU consumption and latency by limiting returned data.
            limit: Maximum items to return
            last_key: Pagination token from previous query
            
        Returns:
            Tuple of (pipeline_list, next_page_token)
        """
        query_kwargs = {
            'IndexName': 'EnvironmentIndex', 
            'KeyConditionExpression': Key('environment').eq(environment),
            'ScanIndexForward': False  # Most recently created first
        }
        
        # Handle projection safely - reduces RCUs and latency
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
                item_to_model(item, PipelineConfigView)
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in query_by_environment: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to query pipelines by environment: {e}", e) from e

    def query_by_environment_and_status(
        self,
        environment: str,
        is_active: bool,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineConfigView], Optional[dict]]:
        """
        Query pipeline configurations by environment and active status.
        
        DynamoDB Operation: Query on EnvironmentIndex GSI with FilterExpression
        GSI Structure: PK=environment, SK=created_at + server-side filtering
        
        Args:
            environment: Environment to filter by
            is_active: Active status to filter by
            projection: Fields to return (uses default minimal projection if None).
                       Projections reduce RCU consumption and latency by limiting returned data.
            limit: Maximum items to return
            last_key: Pagination token from previous query
            
        Returns:
            Tuple of (pipeline_list, next_page_token)
        """
        query_kwargs = {
            'IndexName': 'EnvironmentIndex',
            'KeyConditionExpression': Key('environment').eq(environment),
            'FilterExpression': Attr('is_active').eq(str(is_active).lower()),
            'ScanIndexForward': False
        }
        
        # Handle projection safely - reduces RCUs and latency
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
                item_to_model(item, PipelineConfigView)
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in query_by_environment_and_status: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to query pipelines by environment and status: {e}", e) from e

    def scan_for_all_pipelines(
        self,
        projection: Optional[List[str]] = None,
        limit: int = 100,
        last_key: Optional[dict] = None
    ) -> Tuple[List[PipelineConfigView], Optional[dict]]:
        """
        Scan all pipeline configurations (DISCOURAGED - use queries when possible).
        
        DynamoDB Operation: Scan with required projection and limit
        
        ⚠️ WARNING: This is an expensive operation. Consider using:
        - query_by_environment() for environment-specific queries
        - query_active_pipelines() for active pipeline queries
        
        Args:
            projection: REQUIRED - Fields to return to limit data transfer.
                       Projections are critical for scan operations to reduce RCUs and costs.
            limit: REQUIRED - Maximum items per page (default: 100)
            last_key: Pagination token from previous scan        Returns:
            Tuple of (pipeline_list, next_page_token)
        """
        if not projection:
            projection = self.default_projection
            logger.warning("scan_for_all_pipelines: Using default projection to prevent excessive data transfer")
        
        scan_kwargs = {
            'Limit': limit
        }
        
        # Handle projection safely - critical for scan operations to reduce cost
        proj_expr, expr_names = build_projection_expression(projection)
        if proj_expr:
            scan_kwargs['ProjectionExpression'] = proj_expr
            scan_kwargs['ExpressionAttributeNames'] = expr_names
        
        if last_key:
            scan_kwargs['ExclusiveStartKey'] = last_key
            
        try:
            response = self.gateway.scan(**scan_kwargs)
            
            items = [
                item_to_model(item, PipelineConfigView)
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in scan_for_all_pipelines: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to scan all pipelines: {e}", e) from e

    def get_pipeline_summary(
        self,
        pipeline_id: str
    ) -> Optional[PipelineConfigSummaryView]:
        """
        Get minimal pipeline summary for dashboard/list views.
        
        DynamoDB Operation: GetItem with minimal projection
        
        Args:
            pipeline_id: Pipeline identifier
            
        Returns:
            PipelineConfigSummaryView if found, None otherwise
        """
        summary_projection = [
            'pipeline_id', 'pipeline_name', 'is_active', 
            'environment', 'updated_at'
        ]
        
        proj_expr, expr_names = build_projection_expression(summary_projection)
        get_kwargs = {
            'Key': {'pipeline_id': pipeline_id}
        }
        
        if proj_expr:
            get_kwargs['ProjectionExpression'] = proj_expr
            get_kwargs['ExpressionAttributeNames'] = expr_names
        
        response = self.gateway.table.get_item(**get_kwargs)
        
        if 'Item' not in response:
            return None
            
        return item_to_model(response['Item'], PipelineConfigSummaryView)

    def count_pipelines_by_environment(self, environment: str) -> int:
        """
        Count pipelines in a specific environment.
        
        DynamoDB Operation: Query on EnvironmentIndex GSI with Select=COUNT
        
        Args:
            environment: Environment to count
            
        Returns:
            Number of pipelines in environment
        """
        try:
            response = self.gateway.query(
                IndexName='EnvironmentIndex',
                KeyConditionExpression=Key('environment').eq(environment),
                Select='COUNT'
            )
            
            return response.get('Count', 0)
        except Exception as e:
            logger.error(f"Error in count_pipelines_by_environment: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to count pipelines by environment: {e}", e) from e

    def count_active_pipelines(self) -> int:
        """
        Count active pipelines across all environments.
        
        DynamoDB Operation: Query on ActivePipelinesIndex GSI with Select=COUNT
        
        Returns:
            Number of active pipelines
        """
        try:
            response = self.gateway.query(
                IndexName='ActivePipelinesIndex',
                KeyConditionExpression=Key('is_active').eq('true'),
                Select='COUNT'
            )
            
            return response.get('Count', 0)
        except Exception as e:
            logger.error(f"Error in count_active_pipelines: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to count active pipelines: {e}", e) from e