"""
Comprehensive tests for DynamoDB error mapping.

This module tests the comprehensive error mapping functionality identified
in TEST_ANALYSIS.md, ensuring all botocore exceptions are properly mapped
to domain-specific exceptions with proper context preservation.
"""

import pytest
from unittest.mock import Mock
from botocore.exceptions import ClientError

from dynamodb_wrapper.core.table_gateway import map_dynamodb_error
from dynamodb_wrapper.exceptions import (
    ConnectionError, ConflictError, ItemNotFoundError, 
    ValidationError, RetryableError
)


def create_client_error(error_code: str, message: str = "Test error") -> ClientError:
    """Helper to create ClientError for testing."""
    return ClientError(
        error_response={
            'Error': {
                'Code': error_code,
                'Message': message
            }
        },
        operation_name='TestOperation'
    )


class TestConditionalAndConflictErrors:
    """Test mapping of conditional check and conflict errors."""
    
    def test_conditional_check_failed(self):
        """Test ConditionalCheckFailedException mapping."""
        error = create_client_error('ConditionalCheckFailedException', 'The conditional request failed')
        
        result = map_dynamodb_error(error, 'PutItem', 'test_table', 'test-id')
        
        assert isinstance(result, ConflictError)
        assert 'test-id' in str(result)
        assert 'conditional request failed' in str(result).lower()
        assert result.original_error == error

    def test_transaction_conflict(self):
        """Test TransactionConflictException mapping."""
        error = create_client_error('TransactionConflictException', 'Transaction conflict detected')
        
        result = map_dynamodb_error(error, 'TransactWriteItems', 'test_table', 'txn-123')
        
        assert isinstance(result, ConflictError)
        assert 'txn-123' in str(result)
        assert result.original_error == error

    def test_resource_in_use(self):
        """Test ResourceInUseException mapping."""
        error = create_client_error('ResourceInUseException', 'Table is being modified')
        
        result = map_dynamodb_error(error, 'UpdateTable', 'test_table')
        
        assert isinstance(result, ConflictError)
        assert 'test_table' in str(result)

    def test_duplicate_transaction(self):
        """Test DuplicateTransactionException mapping."""
        error = create_client_error('DuplicateTransactionException', 'Duplicate transaction ID')
        
        result = map_dynamodb_error(error, 'TransactWriteItems', 'test_table', 'dup-txn')
        
        assert isinstance(result, ConflictError)
        assert 'dup-txn' in str(result)

    def test_table_already_exists(self):
        """Test TableAlreadyExistsException mapping."""
        error = create_client_error('TableAlreadyExistsException', 'Table already exists')
        
        result = map_dynamodb_error(error, 'CreateTable', 'existing_table')
        
        assert isinstance(result, ConflictError)
        assert 'existing_table' in str(result)


class TestNotFoundErrors:
    """Test mapping of resource not found errors."""
    
    def test_resource_not_found_with_resource_id(self):
        """Test ResourceNotFoundException with resource ID."""
        error = create_client_error('ResourceNotFoundException', 'Requested resource not found')
        
        result = map_dynamodb_error(error, 'GetItem', 'test_table', 'missing-item')
        
        assert isinstance(result, ItemNotFoundError)
        assert result.table_name == 'test_table'
        assert 'missing-item' in str(result.key)

    def test_resource_not_found_without_resource_id(self):
        """Test ResourceNotFoundException without resource ID (table not found)."""
        error = create_client_error('ResourceNotFoundException', 'Table not found')
        
        result = map_dynamodb_error(error, 'DescribeTable', 'missing_table')
        
        assert isinstance(result, ConnectionError)
        assert 'Table not found' in str(result)

    def test_table_not_found(self):
        """Test TableNotFoundException mapping."""
        error = create_client_error('TableNotFoundException', 'Table does not exist')
        
        result = map_dynamodb_error(error, 'Query', 'nonexistent_table')
        
        assert isinstance(result, ItemNotFoundError)
        assert result.table_name == 'nonexistent_table'

    def test_index_not_found(self):
        """Test IndexNotFoundException mapping."""
        error = create_client_error('IndexNotFoundException', 'Global Secondary Index not found')
        
        result = map_dynamodb_error(error, 'Query', 'test_table', 'missing_index')
        
        assert isinstance(result, ItemNotFoundError)
        assert result.table_name == 'test_table'
        assert 'missing_index' in str(result.key)

    def test_backup_not_found(self):
        """Test BackupNotFoundException mapping."""
        error = create_client_error('BackupNotFoundException', 'Backup not found')
        
        result = map_dynamodb_error(error, 'DescribeBackup', 'test_table', 'backup-123')
        
        assert isinstance(result, ItemNotFoundError)
        assert 'backup-123' in str(result.key)

    def test_replica_not_found(self):
        """Test ReplicaNotFoundException mapping."""
        error = create_client_error('ReplicaNotFoundException', 'Replica not found')
        
        result = map_dynamodb_error(error, 'DescribeTable', 'global_table', 'replica-region')
        
        assert isinstance(result, ItemNotFoundError)
        assert 'replica-region' in str(result.key)

    def test_global_table_not_found(self):
        """Test GlobalTableNotFoundException mapping."""
        error = create_client_error('GlobalTableNotFoundException', 'Global table not found')
        
        result = map_dynamodb_error(error, 'DescribeGlobalTable', 'test_table', 'global-table-1')
        
        assert isinstance(result, ItemNotFoundError)
        assert 'global-table-1' in str(result.key)


class TestValidationErrors:
    """Test mapping of validation and limit errors."""
    
    def test_validation_exception(self):
        """Test ValidationException mapping."""
        error = create_client_error('ValidationException', 'Invalid parameter value')
        
        result = map_dynamodb_error(error, 'PutItem', 'test_table')
        
        assert isinstance(result, ValidationError)
        assert 'Invalid parameter value' in str(result)
        assert result.original_error == error

    def test_item_collection_size_limit(self):
        """Test ItemCollectionSizeLimitExceededException mapping."""
        error = create_client_error('ItemCollectionSizeLimitExceededException', 'Collection size limit exceeded')
        
        result = map_dynamodb_error(error, 'PutItem', 'test_table')
        
        assert isinstance(result, ValidationError)
        assert 'Collection size limit exceeded' in str(result)

    def test_limit_exceeded(self):
        """Test LimitExceededException mapping."""
        error = create_client_error('LimitExceededException', 'Request limit exceeded')
        
        result = map_dynamodb_error(error, 'Scan', 'test_table')
        
        assert isinstance(result, ValidationError)
        assert 'Request limit exceeded' in str(result)

    def test_idempotent_parameter_mismatch(self):
        """Test IdempotentParameterMismatchException mapping."""
        error = create_client_error('IdempotentParameterMismatchException', 'Idempotent parameter mismatch')
        
        result = map_dynamodb_error(error, 'TransactWriteItems', 'test_table')
        
        assert isinstance(result, ValidationError)
        assert 'Idempotent parameter mismatch' in str(result)

    def test_invalid_restore_time(self):
        """Test InvalidRestoreTimeException mapping."""
        error = create_client_error('InvalidRestoreTimeException', 'Invalid restore time')
        
        result = map_dynamodb_error(error, 'RestoreTableFromBackup', 'test_table')
        
        assert isinstance(result, ValidationError)
        assert 'Invalid restore time' in str(result)

    def test_point_in_time_recovery_unavailable(self):
        """Test PointInTimeRecoveryUnavailableException mapping."""
        error = create_client_error('PointInTimeRecoveryUnavailableException', 'PITR not available')
        
        result = map_dynamodb_error(error, 'RestoreTableToPointInTime', 'test_table')
        
        assert isinstance(result, ValidationError)
        assert 'PITR not available' in str(result)

    def test_continuous_backups_unavailable(self):
        """Test ContinuousBackupsUnavailableException mapping."""
        error = create_client_error('ContinuousBackupsUnavailableException', 'Continuous backups not available')
        
        result = map_dynamodb_error(error, 'UpdateContinuousBackups', 'test_table')
        
        assert isinstance(result, ValidationError)
        assert 'Continuous backups not available' in str(result)


class TestRetryableErrors:
    """Test mapping of retryable errors (throttling, service issues)."""
    
    def test_provisioned_throughput_exceeded(self):
        """Test ProvisionedThroughputExceededException mapping."""
        error = create_client_error('ProvisionedThroughputExceededException', 'Throughput exceeded')
        
        result = map_dynamodb_error(error, 'PutItem', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Throughput exceeded' in str(result)
        assert result.original_error == error

    def test_request_limit_exceeded(self):
        """Test RequestLimitExceeded mapping."""
        error = create_client_error('RequestLimitExceeded', 'Request rate too high')
        
        result = map_dynamodb_error(error, 'Query', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Request rate too high' in str(result)

    def test_throttling_exception(self):
        """Test ThrottlingException mapping."""
        error = create_client_error('ThrottlingException', 'Request was throttled')
        
        result = map_dynamodb_error(error, 'Scan', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Request was throttled' in str(result)

    def test_too_many_requests(self):
        """Test TooManyRequestsException mapping."""
        error = create_client_error('TooManyRequestsException', 'Too many requests')
        
        result = map_dynamodb_error(error, 'BatchWriteItem', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Too many requests' in str(result)

    def test_internal_server_error(self):
        """Test InternalServerError mapping."""
        error = create_client_error('InternalServerError', 'Internal service error')
        
        result = map_dynamodb_error(error, 'GetItem', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Internal service error' in str(result)

    def test_service_unavailable(self):
        """Test ServiceUnavailable mapping."""
        error = create_client_error('ServiceUnavailable', 'Service temporarily unavailable')
        
        result = map_dynamodb_error(error, 'PutItem', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Service temporarily unavailable' in str(result)

    def test_service_timeout(self):
        """Test ServiceTimeout mapping."""
        error = create_client_error('ServiceTimeout', 'Service timed out')
        
        result = map_dynamodb_error(error, 'Query', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Service timed out' in str(result)

    def test_transaction_cancelled(self):
        """Test TransactionCanceledException mapping."""
        error = create_client_error('TransactionCanceledException', 'Transaction was cancelled')
        
        result = map_dynamodb_error(error, 'TransactWriteItems', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Transaction was cancelled' in str(result)

    def test_transaction_in_progress(self):
        """Test TransactionInProgressException mapping."""
        error = create_client_error('TransactionInProgressException', 'Transaction in progress')
        
        result = map_dynamodb_error(error, 'TransactWriteItems', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Transaction in progress' in str(result)

    def test_request_timeout(self):
        """Test RequestTimeoutException mapping."""
        error = create_client_error('RequestTimeoutException', 'Request timed out')
        
        result = map_dynamodb_error(error, 'GetItem', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Request timed out' in str(result)

    def test_request_expired(self):
        """Test RequestExpiredException mapping."""
        error = create_client_error('RequestExpiredException', 'Request expired')
        
        result = map_dynamodb_error(error, 'PutItem', 'test_table')
        
        assert isinstance(result, RetryableError)
        assert 'Request expired' in str(result)


class TestConnectionErrors:
    """Test mapping of connection and authentication errors."""
    
    def test_unrecognized_client(self):
        """Test UnrecognizedClientException mapping."""
        error = create_client_error('UnrecognizedClientException', 'Client not recognized')
        
        result = map_dynamodb_error(error, 'GetItem', 'test_table')
        
        assert isinstance(result, ConnectionError)
        assert 'authentication/authorization failed' in str(result).lower()
        assert result.original_error == error

    def test_access_denied(self):
        """Test AccessDeniedException mapping."""
        error = create_client_error('AccessDeniedException', 'Access denied')
        
        result = map_dynamodb_error(error, 'PutItem', 'test_table')
        
        assert isinstance(result, ConnectionError)
        assert 'authentication/authorization failed' in str(result).lower()

    def test_invalid_endpoint(self):
        """Test InvalidEndpointException mapping."""
        error = create_client_error('InvalidEndpointException', 'Invalid endpoint')
        
        result = map_dynamodb_error(error, 'Query', 'test_table')
        
        assert isinstance(result, ConnectionError)
        assert 'Invalid endpoint' in str(result)

    def test_incomplete_signature(self):
        """Test IncompleteSignatureException mapping."""
        error = create_client_error('IncompleteSignatureException', 'Incomplete signature')
        
        result = map_dynamodb_error(error, 'PutItem', 'test_table')
        
        assert isinstance(result, ConnectionError)
        assert 'Invalid endpoint or signature' in str(result)

    def test_invalid_signature(self):
        """Test InvalidSignatureException mapping."""
        error = create_client_error('InvalidSignatureException', 'Invalid signature')
        
        result = map_dynamodb_error(error, 'GetItem', 'test_table')
        
        assert isinstance(result, ConnectionError)
        assert 'Invalid endpoint or signature' in str(result)

    def test_expired_token(self):
        """Test ExpiredTokenException mapping."""
        error = create_client_error('ExpiredTokenException', 'Security token expired')
        
        result = map_dynamodb_error(error, 'Scan', 'test_table')
        
        assert isinstance(result, ConnectionError)
        assert 'Token expired' in str(result)

    def test_token_refresh_required(self):
        """Test TokenRefreshRequiredException mapping."""
        error = create_client_error('TokenRefreshRequiredException', 'Token refresh required')
        
        result = map_dynamodb_error(error, 'Query', 'test_table')
        
        assert isinstance(result, ConnectionError)
        assert 'Token expired' in str(result)


class TestUnknownErrorMapping:
    """Test handling of unknown error codes."""
    
    def test_unknown_error_code(self):
        """Test that unknown error codes map to ConnectionError."""
        error = create_client_error('UnknownErrorCode', 'Unknown error')
        
        result = map_dynamodb_error(error, 'SomeOperation', 'test_table', 'resource-123')
        
        assert isinstance(result, ConnectionError)
        assert 'DynamoDB operation failed' in str(result)
        assert 'resource-123' in str(result)
        assert result.original_error == error

    def test_context_preservation(self):
        """Test that error context is properly preserved."""
        error = create_client_error('ValidationException', 'Test validation error')
        
        result = map_dynamodb_error(error, 'UpdateItem', 'my_table', 'item-456')
        
        # Should include operation, table, and resource in context
        error_str = str(result)
        assert 'UpdateItem' in error_str
        assert 'my_table' in error_str
        assert 'item-456' in error_str
        assert 'Test validation error' in error_str

    def test_context_without_resource_id(self):
        """Test error context when no resource ID is provided."""
        error = create_client_error('ServiceException', 'Service error')
        
        result = map_dynamodb_error(error, 'DescribeTable', 'test_table')
        
        error_str = str(result)
        assert 'DescribeTable' in error_str
        assert 'test_table' in error_str
        assert 'Service error' in error_str
        # Should not contain resource info
        assert 'resource:' not in error_str


class TestErrorChaining:
    """Test that original errors are properly chained."""
    
    def test_original_error_preserved(self):
        """Test that original ClientError is preserved in all exception types."""
        original_error = create_client_error('ConditionalCheckFailedException', 'Original error')
        
        result = map_dynamodb_error(original_error, 'PutItem', 'test_table')
        
        assert result.original_error == original_error
        assert isinstance(result.original_error, ClientError)

    def test_exception_chaining_for_different_types(self):
        """Test exception chaining for different error types."""
        errors_and_types = [
            ('ConditionalCheckFailedException', ConflictError),
            ('ValidationException', ValidationError),
            ('ProvisionedThroughputExceededException', RetryableError),
            ('ResourceNotFoundException', ItemNotFoundError),
            ('AccessDeniedException', ConnectionError)
        ]
        
        for error_code, expected_type in errors_and_types:
            error = create_client_error(error_code, f'Test {error_code}')
            result = map_dynamodb_error(error, 'TestOp', 'test_table', 'test-resource')
            
            assert isinstance(result, expected_type)
            assert result.original_error == error
            assert isinstance(result.original_error, ClientError)