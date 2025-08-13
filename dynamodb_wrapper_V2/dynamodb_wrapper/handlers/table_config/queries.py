"""
Table Configuration Read API

This module provides optimized read operations for table configurations using:
- GSI queries for pipeline and table type filtering
- Projection expressions for minimal data transfer
- Pagination support with last_key tokens
- Server-side filtering for active status

All methods return (items, last_key) tuples for proper pagination handling.
"""

import logging
from typing import List, Optional, Tuple

from boto3.dynamodb.conditions import Key, Attr

from ...config import DynamoDBConfig
from ...models import TableConfig, TableType, TableConfigView, TableConfigSummaryView
from ...utils import item_to_model, build_projection_expression
from ...core import create_table_gateway

logger = logging.getLogger(__name__)


class TableConfigReadApi:
    """
    Read-only API for table configuration queries.
    
    Uses optimized DynamoDB access patterns:
    - GSI queries for pipeline and table type filtering
    - Projection expressions to minimize data transfer
    - Server-side filtering with FilterExpression
    - Proper pagination with ExclusiveStartKey
    """

    def __init__(self, config: DynamoDBConfig):
        """Initialize read API with configuration."""
        self.config = config
        self.gateway = create_table_gateway(config, "table_config")
        
        # Default minimal projection for list operations
        self.default_projection = [
            'table_id', 'pipeline_id', 'table_name', 'table_type', 
            'data_format', 'location', 'is_active', 'created_at', 'updated_at'
        ]

    def get_by_id(
        self,
        table_id: str,
        projection: Optional[List[str]] = None
    ) -> Optional[TableConfigView]:
        """
        Get table configuration by ID (optimized read model).
        
        DynamoDB Operation: GetItem with primary key
        
        Args:
            table_id: Table identifier
            projection: Fields to return (uses view projection if None).
                       Projections reduce RCU consumption and latency by limiting returned data.
            
        Returns:
            TableConfigView if found, None otherwise
        """
        get_kwargs = {
            'Key': {'table_id': table_id}
        }
        
        # Use view projection if none specified (optimized for read operations)
        if projection:
            proj_expr, expr_names = build_projection_expression(projection)
        else:
            # Use optimized projection for TableConfigView
            view_projection = [
                'table_id', 'pipeline_id', 'table_name', 'table_type', 'data_format',
                'location', 'environment', 'is_active', 'description', 'tags',
                'last_updated_data', 'record_count', 'size_bytes',
                'created_at', 'updated_at', 'created_by', 'updated_by'
            ]
            proj_expr, expr_names = build_projection_expression(view_projection)
            
        if proj_expr:
            get_kwargs['ProjectionExpression'] = proj_expr
            get_kwargs['ExpressionAttributeNames'] = expr_names
            
        response = self.gateway.table.get_item(**get_kwargs)
        
        if 'Item' not in response:
            return None
            
        return item_to_model(response['Item'], TableConfigView)

    def query_by_pipeline(
        self,
        pipeline_id: str,
        table_type: Optional[TableType] = None,
        active_only: bool = False,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[TableConfigView], Optional[dict]]:
        """
        Query table configurations by pipeline.
        
        DynamoDB Operation: Query on PipelineTablesIndex GSI
        GSI Structure: PK=pipeline_id, SK=table_type#table_id
        
        Args:
            pipeline_id: Pipeline identifier
            table_type: Optional table type filter
            active_only: Filter for active tables only
            projection: Fields to return (uses default minimal projection if None).
                       Projections reduce RCU consumption and latency by limiting returned data.
            limit: Maximum items to return
            last_key: Pagination token from previous query
            
        Returns:
            Tuple of (table_list, next_page_token)
        """
        query_kwargs = {
            'IndexName': 'PipelineTablesIndex',
            'KeyConditionExpression': Key('pipeline_id').eq(pipeline_id)
        }
        
        # Handle projection safely - reduces RCUs and latency
        proj_expr, expr_names = build_projection_expression(projection or self.default_projection)
        if proj_expr:
            query_kwargs['ProjectionExpression'] = proj_expr
            query_kwargs['ExpressionAttributeNames'] = expr_names
        
        # Add table type filter to KeyCondition if specified
        if table_type:
            # Assuming SK format is: table_type#table_id
            query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & \
                Key('table_type_table_id').begins_with(f"{table_type.value}#")
        
        # Add active filter if needed
        if active_only:
            query_kwargs['FilterExpression'] = Attr('is_active').eq('true')
            
        if limit:
            query_kwargs['Limit'] = limit
        if last_key:
            query_kwargs['ExclusiveStartKey'] = last_key
            
        try:
            response = self.gateway.query(**query_kwargs)
            
            items = [
                item_to_model(item, TableConfigView)
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in query_by_pipeline: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to query table configs by pipeline: {e}", e) from e

    def query_by_table_type(
        self,
        table_type: TableType,
        pipeline_id: Optional[str] = None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[TableConfigView], Optional[dict]]:
        """
        Query table configurations by table type.
        
        DynamoDB Operation: Query on TableTypeIndex GSI
        GSI Structure: PK=table_type, SK=pipeline_id#table_id
        
        Args:
            table_type: Table type to filter by
            pipeline_id: Optional pipeline ID filter
            projection: Fields to return (uses default minimal projection if None).
                       Projections reduce RCU consumption and latency by limiting returned data.
            limit: Maximum items to return
            last_key: Pagination token from previous query
            
        Returns:
            Tuple of (table_list, next_page_token)
        """
        query_kwargs = {
            'IndexName': 'TableTypeIndex',
            'KeyConditionExpression': Key('table_type').eq(table_type.value)
        }
        
        # Handle projection safely - reduces RCUs and latency
        proj_expr, expr_names = build_projection_expression(projection or self.default_projection)
        if proj_expr:
            query_kwargs['ProjectionExpression'] = proj_expr
            query_kwargs['ExpressionAttributeNames'] = expr_names
        
        # Add pipeline filter to KeyCondition if specified
        if pipeline_id:
            # Assuming SK format is: pipeline_id#table_id
            query_kwargs['KeyConditionExpression'] = query_kwargs['KeyConditionExpression'] & \
                Key('pipeline_table_id').begins_with(f"{pipeline_id}#")
        
        if limit:
            query_kwargs['Limit'] = limit
        if last_key:
            query_kwargs['ExclusiveStartKey'] = last_key
            
        try:
            response = self.gateway.query(**query_kwargs)
            
            items = [
                item_to_model(item, TableConfigView)
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in query_by_table_type: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to query table configs by table type: {e}", e) from e

    def get_source_tables(
        self,
        pipeline_id: str,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[TableConfigView], Optional[dict]]:
        """
        Get source tables for a pipeline.
        
        DynamoDB Operation: Query on PipelineTablesIndex GSI with table type filter
        
        Args:
            pipeline_id: Pipeline identifier
            projection: Fields to return
            limit: Maximum items to return
            last_key: Pagination token
            
        Returns:
            Tuple of (source_table_list, next_page_token)
        """
        return self.query_by_pipeline(
            pipeline_id=pipeline_id,
            table_type=TableType.SOURCE,
            projection=projection,
            limit=limit,
            last_key=last_key
        )

    def get_destination_tables(
        self,
        pipeline_id: str,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[TableConfigView], Optional[dict]]:
        """
        Get destination tables for a pipeline.
        
        DynamoDB Operation: Query on PipelineTablesIndex GSI with table type filter
        
        Args:
            pipeline_id: Pipeline identifier
            projection: Fields to return
            limit: Maximum items to return
            last_key: Pagination token
            
        Returns:
            Tuple of (destination_table_list, next_page_token)
        """
        return self.query_by_pipeline(
            pipeline_id=pipeline_id,
            table_type=TableType.DESTINATION,
            projection=projection,
            limit=limit,
            last_key=last_key
        )

    def get_active_tables_by_pipeline(
        self,
        pipeline_id: str,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[TableConfigView], Optional[dict]]:
        """
        Get active table configurations for a pipeline.
        
        DynamoDB Operation: Query on PipelineTablesIndex GSI with FilterExpression
        
        Args:
            pipeline_id: Pipeline identifier
            projection: Fields to return
            limit: Maximum items to return
            last_key: Pagination token
            
        Returns:
            Tuple of (active_table_list, next_page_token)
        """
        return self.query_by_pipeline(
            pipeline_id=pipeline_id,
            active_only=True,
            projection=projection,
            limit=limit,
            last_key=last_key
        )

    def get_tables_by_format(
        self,
        data_format: str,
        pipeline_id: Optional[str] = None,
        projection: Optional[List[str]] = None,
        limit: Optional[int] = None,
        last_key: Optional[dict] = None
    ) -> Tuple[List[TableConfigView], Optional[dict]]:
        """
        Get tables by data format using scan with filter.
        
        DynamoDB Operation: Scan with FilterExpression
        ⚠️ Less efficient than GSI queries - use sparingly
        
        Args:
            data_format: Data format to filter by (parquet, json, etc.)
            pipeline_id: Optional pipeline filter
            projection: Fields to return
            limit: Maximum items to return
            last_key: Pagination token
            
        Returns:
            Tuple of (table_list, next_page_token)
        """
        scan_kwargs = {
            'FilterExpression': Attr('data_format').eq(data_format)
        }
        
        # Handle projection safely - critical for scan operations
        proj_expr, expr_names = build_projection_expression(projection or self.default_projection)
        if proj_expr:
            scan_kwargs['ProjectionExpression'] = proj_expr
            scan_kwargs['ExpressionAttributeNames'] = expr_names
        
        # Add pipeline filter if specified
        if pipeline_id:
            scan_kwargs['FilterExpression'] = scan_kwargs['FilterExpression'] & \
                Attr('pipeline_id').eq(pipeline_id)
        
        if limit:
            scan_kwargs['Limit'] = limit
        if last_key:
            scan_kwargs['ExclusiveStartKey'] = last_key
            
        try:
            response = self.gateway.scan(**scan_kwargs)
            
            items = [
                item_to_model(item, TableConfigView)
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in get_tables_by_format: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to scan tables by format: {e}", e) from e

    def scan_for_all_tables(
        self,
        projection: Optional[List[str]] = None,
        limit: int = 100,
        last_key: Optional[dict] = None
    ) -> Tuple[List[TableConfigView], Optional[dict]]:
        """
        Scan all table configurations (DISCOURAGED - use queries when possible).
        
        DynamoDB Operation: Scan with required projection and limit
        
        ⚠️ WARNING: This is an expensive operation. Consider using:
        - query_by_pipeline() for pipeline-specific queries
        - query_by_table_type() for type-specific queries
        
        Args:
            projection: REQUIRED - Fields to return to limit data transfer.
                       Projections are critical for scan operations to reduce RCUs and costs.
            limit: REQUIRED - Maximum items per page (default: 100)
            last_key: Pagination token from previous scan
            
        Returns:
            Tuple of (table_list, next_page_token)
        """
        if not projection:
            projection = self.default_projection
            logger.warning("scan_for_all_tables: Using default projection to prevent excessive data transfer")
        
        scan_kwargs = {
            'Limit': limit
        }
        
        # Handle projection safely - reduces scan costs significantly
        proj_expr, expr_names = build_projection_expression(projection)
        if proj_expr:
            scan_kwargs['ProjectionExpression'] = proj_expr
            scan_kwargs['ExpressionAttributeNames'] = expr_names
        
        if last_key:
            scan_kwargs['ExclusiveStartKey'] = last_key
            
        try:
            response = self.gateway.scan(**scan_kwargs)
            
            items = [
                item_to_model(item, TableConfigView)
                for item in response.get('Items', [])
            ]
            
            next_key = response.get('LastEvaluatedKey')
            return items, next_key
        except Exception as e:
            logger.error(f"Error in scan_for_all_tables: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to scan all tables: {e}", e) from e

    def get_table_summary(
        self,
        table_id: str
    ) -> Optional[TableConfigSummaryView]:
        """
        Get minimal table summary for dashboard/list views.
        
        DynamoDB Operation: GetItem with minimal projection
        
        Args:
            table_id: Table identifier
            
        Returns:
            TableConfigSummaryView if found, None otherwise
        """
        summary_projection = [
            'table_id', 'table_name', 'table_type', 'data_format', 
            'is_active', 'record_count'
        ]
        
        proj_expr, expr_names = build_projection_expression(summary_projection)
        get_kwargs = {
            'Key': {'table_id': table_id}
        }
        
        if proj_expr:
            get_kwargs['ProjectionExpression'] = proj_expr
            get_kwargs['ExpressionAttributeNames'] = expr_names
        
        response = self.gateway.table.get_item(**get_kwargs)
        
        if 'Item' not in response:
            return None
            
        return item_to_model(response['Item'], TableConfigSummaryView)

    def count_tables_by_pipeline(self, pipeline_id: str) -> int:
        """
        Count tables in a specific pipeline.
        
        DynamoDB Operation: Query on PipelineTablesIndex GSI with Select=COUNT
        
        Args:
            pipeline_id: Pipeline to count tables for
            
        Returns:
            Number of tables in pipeline
        """
        try:
            response = self.gateway.query(
                IndexName='PipelineTablesIndex',
                KeyConditionExpression=Key('pipeline_id').eq(pipeline_id),
                Select='COUNT'
            )
            
            return response.get('Count', 0)
        except Exception as e:
            logger.error(f"Error in count_tables_by_pipeline: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to count tables by pipeline: {e}", e) from e

    def count_tables_by_type(self, table_type: TableType) -> int:
        """
        Count tables of a specific type.
        
        DynamoDB Operation: Query on TableTypeIndex GSI with Select=COUNT
        
        Args:
            table_type: Table type to count
            
        Returns:
            Number of tables of specified type
        """
        try:
            response = self.gateway.query(
                IndexName='TableTypeIndex',
                KeyConditionExpression=Key('table_type').eq(table_type.value),
                Select='COUNT'
            )
            
            return response.get('Count', 0)
        except Exception as e:
            logger.error(f"Error in count_tables_by_type: {e}")
            from ...exceptions import ConnectionError
            raise ConnectionError(f"Failed to count tables by type: {e}", e) from e

    def get_tables_statistics_summary(
        self, 
        pipeline_id: Optional[str] = None
    ) -> dict:
        """
        Get aggregated table statistics.
        
        Args:
            pipeline_id: Optional pipeline filter
            
        Returns:
            Dictionary with aggregated statistics
        """
        if pipeline_id:
            tables, _ = self.query_by_pipeline(
                pipeline_id,
                projection=['record_count', 'size_bytes', 'table_type'],
                limit=1000  # Reasonable limit for aggregation
            )
        else:
            tables, _ = self.scan_for_all_tables(
                projection=['record_count', 'size_bytes', 'table_type'],
                limit=1000
            )
        
        # Aggregate statistics
        stats = {
            'total_tables': len(tables),
            'total_records': 0,
            'total_size_bytes': 0,
            'by_type': {}
        }
        
        for table in tables:
            if table.record_count:
                stats['total_records'] += table.record_count
            if table.size_bytes:
                stats['total_size_bytes'] += table.size_bytes
                
            table_type = table.table_type.value if table.table_type else 'unknown'
            if table_type not in stats['by_type']:
                stats['by_type'][table_type] = 0
            stats['by_type'][table_type] += 1
            
        return stats