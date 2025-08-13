"""
Thin DynamoDB Table Gateway

This module provides a lightweight wrapper around boto3 DynamoDB operations.
Unlike the previous generic repository pattern, this gateway:

1. Avoids god-interfaces by exposing only essential DynamoDB operations
2. Exposes native DynamoDB power through raw query/update methods
3. Fits CQRS by being a building block for specialized read/write APIs

Design Philosophy:
- BAD: get_all() - Hides complexity, encourages inefficient full table scans
- GOOD: query_by_env(env, projection=['name'], limit=50, last_key=...) - Explicit, optimized

The gateway focuses on:
- Creating boto3 Table handles
- Common helpers (projection, pagination, error mapping)
- Conditional/transaction/batch operation wrappers
- Raw escape hatches for advanced DynamoDB features

This approach allows read/write APIs to compose operations efficiently without
inheriting heavyweight base classes or being locked into generic CRUD patterns.
"""

import logging
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from ..config import DynamoDBConfig
from ..exceptions import (
    ConnectionError,
    ConflictError,
    ItemNotFoundError,
    ValidationError,
    RetryableError
)

logger = logging.getLogger(__name__)


def map_dynamodb_error(
    error: ClientError, 
    operation: str, 
    table_name: str, 
    resource_id: Optional[str] = None
) -> Exception:
    """Map DynamoDB ClientError to domain-specific exceptions.
    
    This function provides consistent error mapping across all DynamoDB operations,
    converting boto3 ClientErrors into meaningful domain exceptions.
    
    Args:
        error: The boto3 ClientError
        operation: The operation that failed (e.g., "GetItem", "PutItem")
        table_name: The DynamoDB table name
        resource_id: Optional resource identifier for context
        
    Returns:
        Appropriate domain exception
        
    Raises:
        ConnectionError: For network/service issues
        ConflictError: For conditional check failures
        ItemNotFoundError: For missing items
        ValidationError: For validation failures
        RetryableError: For throttling/capacity issues
    """
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']
    
    # Build context for error message
    context = f"{operation} on {table_name}"
    if resource_id:
        context += f" (resource: {resource_id})"
    
    full_message = f"{context}: {error_message}"
    
    # Map specific DynamoDB errors to domain exceptions
    if error_code == 'ConditionalCheckFailedException':
        return ConflictError(f"Conditional check failed - {full_message}", resource_id, original_error=error)
    
    elif error_code == 'ResourceNotFoundException':
        if resource_id:
            # ItemNotFoundError expects table_name and key, so we'll use a generic format
            return ItemNotFoundError(table_name, {'resource_id': resource_id}, original_error=error)
        else:
            return ConnectionError(f"Table not found - {full_message}", original_error=error)
    
    elif error_code == 'ValidationException':
        return ValidationError(f"Validation failed - {full_message}", original_error=error)
    
    elif error_code in ['ProvisionedThroughputExceededException', 'RequestLimitExceeded']:
        return RetryableError(f"Throttling - {full_message}", original_error=error)
    
    elif error_code in ['InternalServerError', 'ServiceUnavailable']:
        return RetryableError(f"Service unavailable - {full_message}", original_error=error)
    
    elif error_code in ['UnrecognizedClientException', 'AccessDeniedException']:
        return ConnectionError(f"Authentication/authorization failed - {full_message}", original_error=error)
    
    elif error_code == 'ItemCollectionSizeLimitExceededException':
        return ValidationError(f"Item collection size limit exceeded - {full_message}", original_error=error)
    
    elif error_code == 'TransactionConflictException':
        return ConflictError(f"Transaction conflict - {full_message}", resource_id, original_error=error)
    
    elif error_code in ['TransactionCanceledException', 'TransactionInProgressException']:
        return RetryableError(f"Transaction issue - {full_message}", original_error=error)
    
    # Additional comprehensive error mappings
    elif error_code == 'LimitExceededException':
        return ValidationError(f"DynamoDB limit exceeded - {full_message}", original_error=error)
    
    elif error_code == 'ResourceInUseException':
        return ConflictError(f"Resource in use - {full_message}", resource_id, original_error=error)
    
    elif error_code == 'BackupNotFoundException':
        return ItemNotFoundError(table_name, {'backup_id': resource_id}, original_error=error)
    
    elif error_code == 'TableNotFoundException':
        return ItemNotFoundError(table_name, {'table_name': table_name}, original_error=error)
    
    elif error_code == 'IndexNotFoundException':
        return ItemNotFoundError(table_name, {'index_name': resource_id}, original_error=error)
    
    elif error_code in ['TableAlreadyExistsException', 'BackupInUseException']:
        return ConflictError(f"Resource already exists - {full_message}", resource_id, original_error=error)
    
    elif error_code in ['InvalidEndpointException', 'IncompleteSignatureException', 'InvalidSignatureException']:
        return ConnectionError(f"Invalid endpoint or signature - {full_message}", original_error=error)
    
    elif error_code in ['ExpiredTokenException', 'TokenRefreshRequiredException']:
        return ConnectionError(f"Token expired - {full_message}", original_error=error)
    
    elif error_code == 'DuplicateTransactionException':
        return ConflictError(f"Duplicate transaction - {full_message}", resource_id, original_error=error)
    
    elif error_code == 'IdempotentParameterMismatchException':
        return ValidationError(f"Idempotent parameter mismatch - {full_message}", original_error=error)
    
    elif error_code in ['RequestTimeoutException', 'RequestExpiredException']:
        return RetryableError(f"Request timeout - {full_message}", original_error=error)
    
    elif error_code == 'InvalidRestoreTimeException':
        return ValidationError(f"Invalid restore time - {full_message}", original_error=error)
    
    elif error_code == 'PointInTimeRecoveryUnavailableException':
        return ValidationError(f"Point-in-time recovery unavailable - {full_message}", original_error=error)
    
    elif error_code == 'ContinuousBackupsUnavailableException':
        return ValidationError(f"Continuous backups unavailable - {full_message}", original_error=error)
    
    elif error_code == 'ReplicaNotFoundException':
        return ItemNotFoundError(table_name, {'replica': resource_id}, original_error=error)
    
    elif error_code == 'GlobalTableNotFoundException':
        return ItemNotFoundError(table_name, {'global_table': resource_id}, original_error=error)
    
    elif error_code in ['ReplicaAlreadyExistsException', 'GlobalTableAlreadyExistsException']:
        return ConflictError(f"Replica/Global table already exists - {full_message}", resource_id, original_error=error)
    
    # Network and throttling errors that should be retried
    elif error_code in [
        'ThrottlingException', 'SlowDown', 'BandwidthLimitExceeded',
        'RequestThrottledException', 'TooManyRequestsException'
    ]:
        return RetryableError(f"Throttling/rate limiting - {full_message}", original_error=error)
    
    # Service errors that may be retryable
    elif error_code in [
        'ServiceException', 'ServiceUnavailableException', 'InternalFailure',
        'ServiceFailureException', 'ServiceTimeout'
    ]:
        return RetryableError(f"Service error - {full_message}", original_error=error)
    
    # Default to ConnectionError for unknown errors
    logger.warning(f"Unknown DynamoDB error code '{error_code}' mapped to ConnectionError")
    return ConnectionError(f"DynamoDB operation failed - {full_message}", original_error=error)


class TableGateway:
    """
    Thin gateway for DynamoDB table operations.
    
    Provides minimal, composable DynamoDB operations without generic CRUD assumptions.
    Designed to be used by specialized read/write APIs rather than directly by clients.
    
    Key principles:
    - Expose native DynamoDB capabilities
    - Avoid abstraction layers that hide performance characteristics
    - Enable CQRS by providing building blocks, not complete solutions
    - Support advanced DynamoDB features through escape hatches
    """

    def __init__(self, config: DynamoDBConfig, table_name: str):
        """Initialize table gateway.

        Args:
            config: DynamoDB configuration
            table_name: Name of the DynamoDB table
        """
        self.config = config
        self.table_name = table_name
        self._dynamodb = None
        self._table = None

    @property
    def dynamodb(self):
        """Lazy initialization of DynamoDB resource."""
        if self._dynamodb is None:
            try:
                session = boto3.Session(
                    aws_access_key_id=self.config.aws_access_key_id,
                    aws_secret_access_key=self.config.aws_secret_access_key,
                    region_name=self.config.region_name
                )

                # Configure connection parameters
                dynamodb_config = {
                    'region_name': self.config.region_name
                }

                if self.config.endpoint_url:
                    dynamodb_config['endpoint_url'] = self.config.endpoint_url

                # Add retry and timeout configuration
                boto_config = Config(
                    retries={'max_attempts': self.config.retries},
                    max_pool_connections=self.config.max_pool_connections,
                    read_timeout=self.config.timeout_seconds,
                    connect_timeout=self.config.timeout_seconds
                )
                dynamodb_config['config'] = boto_config

                self._dynamodb = session.resource('dynamodb', **dynamodb_config)
            except Exception as e:
                logger.error(f"Failed to create DynamoDB resource: {e}")
                raise ConnectionError(f"Failed to connect to DynamoDB: {e}", e) from e
        return self._dynamodb

    @property
    def table(self):
        """
        Get boto3 DynamoDB Table resource.
        
        This is the primary interface for DynamoDB operations.
        Read/write APIs use this directly for maximum flexibility.
        """
        if self._table is None:
            try:
                self._table = self.dynamodb.Table(self.table_name)
            except Exception as e:
                logger.error(f"Failed to access table '{self.table_name}': {e}")
                raise ConnectionError(f"Failed to access table '{self.table_name}': {e}", e) from e
        return self._table

    def query(self, **kwargs) -> Dict[str, Any]:
        """
        Execute DynamoDB Query operation.
        
        Raw pass-through to boto3 with error handling. Use this for:
        - GSI queries with KeyConditionExpression
        - Efficient pagination with ExclusiveStartKey
        - Projection expressions for minimal data transfer
        - Filter expressions for server-side filtering
        
        Args:
            **kwargs: All boto3 query parameters
            
        Returns:
            Raw DynamoDB response
            
        Example:
            response = gateway.query(
                IndexName='ActivePipelinesIndex',
                KeyConditionExpression=Key('is_active').eq(True),
                ProjectionExpression='pipeline_id, pipeline_name',
                Limit=50
            )
        """
        try:
            return self.table.query(**kwargs)
        except ClientError as e:
            raise map_dynamodb_error(e, "Query", self.table_name) from e

    def scan(self, **kwargs) -> Dict[str, Any]:
        """
        Execute DynamoDB Scan operation.
        
        ⚠️  DISCOURAGED: Scan operations are inefficient and expensive.
        Only use when absolutely necessary and always with:
        - ProjectionExpression to limit data transfer
        - Limit to prevent runaway costs
        - Proper pagination handling
        
        Args:
            **kwargs: All boto3 scan parameters
            
        Returns:
            Raw DynamoDB response
            
        Note:
            Consider using Query with GSI instead of Scan whenever possible.
        """
        try:
            # Require explicit projection and limit for scan operations
            if 'ProjectionExpression' not in kwargs:
                logger.warning(f"Scan on {self.table_name} without ProjectionExpression - consider adding one")
            if 'Limit' not in kwargs:
                logger.warning(f"Scan on {self.table_name} without Limit - consider adding one")
                
            return self.table.scan(**kwargs)
        except ClientError as e:
            raise map_dynamodb_error(e, "Scan", self.table_name) from e

    def put_item(self, item: Dict[str, Any], condition_expression=None) -> None:
        """
        Put item into DynamoDB table.
        
        Args:
            item: Item to store
            condition_expression: Optional condition for put operation
            
        Example:
            gateway.put_item(
                item={'pipeline_id': 'test', 'name': 'Test Pipeline'},
                condition_expression=Attr('pipeline_id').not_exists()
            )
        """
        try:
            put_kwargs = {'Item': item}
            if condition_expression is not None:
                put_kwargs['ConditionExpression'] = condition_expression
                
            self.table.put_item(**put_kwargs)
            logger.info(f"Put item in {self.table_name}: {item}")
        except ClientError as e:
            # Extract resource_id if available for better error context
            resource_id = item.get('pipeline_id') or item.get('table_id') or item.get('run_id')
            raise map_dynamodb_error(e, "PutItem", self.table_name, resource_id) from e

    def update_item(
        self, 
        key: Dict[str, Any], 
        update_expression: str,
        expression_attribute_values: Optional[Dict[str, Any]] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        condition_expression=None,
        return_values: str = 'NONE'
    ) -> Optional[Dict[str, Any]]:
        """
        Update item in DynamoDB table.
        
        Args:
            key: Primary key of item to update
            update_expression: UPDATE expression
            expression_attribute_values: Values for update expression
            expression_attribute_names: Names for update expression
            condition_expression: Optional condition for update
            return_values: What to return after update
            
        Returns:
            Updated attributes if return_values != 'NONE'
        """
        try:
            update_kwargs = {
                'Key': key,
                'UpdateExpression': update_expression,
                'ReturnValues': return_values
            }
            
            if expression_attribute_values:
                update_kwargs['ExpressionAttributeValues'] = expression_attribute_values
            if expression_attribute_names:
                update_kwargs['ExpressionAttributeNames'] = expression_attribute_names
            if condition_expression is not None:
                update_kwargs['ConditionExpression'] = condition_expression
                
            response = self.table.update_item(**update_kwargs)
            logger.info(f"Updated item in {self.table_name}: {key}")
            
            return response.get('Attributes') if return_values != 'NONE' else None
            
        except ClientError as e:
            # Extract resource_id from key for better error context
            resource_id = key.get('pipeline_id') or key.get('table_id') or key.get('run_id')
            raise map_dynamodb_error(e, "UpdateItem", self.table_name, resource_id) from e

    def delete_item(
        self, 
        key: Dict[str, Any], 
        condition_expression=None,
        return_values: str = 'NONE'
    ) -> Optional[Dict[str, Any]]:
        """
        Delete item from DynamoDB table.
        
        Args:
            key: Primary key of item to delete
            condition_expression: Optional condition for delete
            return_values: What to return after delete
            
        Returns:
            Deleted attributes if return_values != 'NONE'
        """
        try:
            delete_kwargs = {
                'Key': key,
                'ReturnValues': return_values
            }
            
            if condition_expression is not None:
                delete_kwargs['ConditionExpression'] = condition_expression
                
            response = self.table.delete_item(**delete_kwargs)
            logger.info(f"Deleted item from {self.table_name}: {key}")
            
            return response.get('Attributes') if return_values != 'NONE' else None
            
        except ClientError as e:
            # Extract resource_id from key for better error context
            resource_id = key.get('pipeline_id') or key.get('table_id') or key.get('run_id')
            raise map_dynamodb_error(e, "DeleteItem", self.table_name, resource_id) from e

    def batch_writer(self):
        """
        Get batch writer for efficient batch operations.
        
        Returns:
            boto3 batch writer context manager
            
        Example:
            with gateway.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)
        """
        return self.table.batch_writer()

    def transact_write_items(self, transact_items: List[Dict[str, Any]]) -> None:
        """
        Execute transactional write operations.
        
        Args:
            transact_items: List of transaction items
            
        Example:
            gateway.transact_write_items([
                {
                    'Put': {
                        'TableName': 'pipeline_config',
                        'Item': {...},
                        'ConditionExpression': 'attribute_not_exists(pipeline_id)'
                    }
                },
                {
                    'Update': {
                        'TableName': 'pipeline_run_logs',
                        'Key': {...},
                        'UpdateExpression': 'SET #status = :status',
                        'ExpressionAttributeNames': {'#status': 'status'},
                        'ExpressionAttributeValues': {':status': 'RUNNING'}
                    }
                }
            ])
        """
        try:
            self.dynamodb.meta.client.transact_write_items(
                TransactItems=transact_items
            )
            logger.info(f"Transaction completed on {self.table_name}")
        except ClientError as e:
            raise map_dynamodb_error(e, "TransactWriteItems", self.table_name) from e

    def raw_query(self, **kwargs) -> Dict[str, Any]:
        """
        Raw DynamoDB Query escape hatch.
        
        Passes all kwargs directly to boto3 without any processing.
        Use this when you need DynamoDB features not covered by other methods.
        
        Args:
            **kwargs: Raw boto3 query parameters
            
        Returns:
            Raw DynamoDB response
        """
        return self.query(**kwargs)

    def raw_update(self, **kwargs) -> Dict[str, Any]:
        """
        Raw DynamoDB UpdateItem escape hatch.
        
        Passes all kwargs directly to boto3 without any processing.
        Use this for advanced update patterns not covered by update_item().
        
        Args:
            **kwargs: Raw boto3 update_item parameters
            
        Returns:
            Raw DynamoDB response
        """
        try:
            return self.table.update_item(**kwargs)
        except ClientError as e:
            raise map_dynamodb_error(e, "RawUpdateItem", self.table_name) from e


def create_table_gateway(config: DynamoDBConfig, table_name: str) -> TableGateway:
    """
    Factory function to create a TableGateway instance.
    
    Args:
        config: DynamoDB configuration
        table_name: Table name (will use config.get_table_name() if needed)
        
    Returns:
        Configured TableGateway instance
    """
    # Use config to get properly prefixed table name
    full_table_name = config.get_table_name(table_name)
    return TableGateway(config, full_table_name)