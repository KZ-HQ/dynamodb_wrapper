"""
Tests for transaction failure edge cases and atomicity.

This module tests transaction atomicity, failure scenarios, and edge cases
identified in TEST_ANALYSIS.md to ensure proper ACID compliance.
"""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from dynamodb_wrapper.handlers.pipeline_config.commands import PipelineConfigWriteApi
from dynamodb_wrapper.exceptions import ConflictError, ConnectionError, RetryableError, ValidationError
from dynamodb_wrapper.config import DynamoDBConfig


@pytest.fixture
def mock_config():
    """Mock DynamoDBConfig for testing."""
    return DynamoDBConfig(
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        table_prefix="test"
    )


@pytest.fixture
def mock_gateway():
    """Mock TableGateway for testing."""
    gateway = Mock()
    gateway.table_name = "test_dev_pipeline_config"
    return gateway


class TestTransactionAtomicity:
    """Test transaction atomicity and all-or-nothing behavior."""

    def test_activate_pipelines_transaction_success(self, mock_config, mock_gateway):
        """Test successful transaction activation of multiple pipelines."""
        # Mock successful transaction
        mock_gateway.transact_write_items.return_value = None
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            pipeline_ids = ["pipeline-1", "pipeline-2", "pipeline-3"]
            updated_by = "test-user"
            
            result = api.activate_pipelines(pipeline_ids, updated_by)
            
            # Should return count of activated pipelines
            assert result == 3
            
            # Should call transact_write_items once
            mock_gateway.transact_write_items.assert_called_once()
            
            # Verify transaction structure
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            assert len(call_args) == 3  # One update per pipeline
            
            # Each transaction item should be an Update operation
            for i, transact_item in enumerate(call_args):
                assert 'Update' in transact_item
                update = transact_item['Update']
                assert update['TableName'] == 'test_dev_pipeline_config'
                assert update['Key']['pipeline_id'] == f'pipeline-{i+1}'
                assert 'attribute_exists(pipeline_id)' in update['ConditionExpression']
                assert update['ExpressionAttributeValues'][':active'] == 'true'
                assert update['ExpressionAttributeValues'][':user'] == 'test-user'

    def test_deactivate_pipelines_transaction_success(self, mock_config, mock_gateway):
        """Test successful transaction deactivation of multiple pipelines."""
        mock_gateway.transact_write_items.return_value = None
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            pipeline_ids = ["pipeline-1", "pipeline-2"]
            updated_by = "admin-user"
            
            result = api.deactivate_pipelines(pipeline_ids, updated_by)
            
            assert result == 2
            mock_gateway.transact_write_items.assert_called_once()
            
            # Verify deactivation transaction structure
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            assert len(call_args) == 2
            
            for transact_item in call_args:
                update = transact_item['Update']
                assert update['ExpressionAttributeValues'][':active'] == 'false'
                assert update['ExpressionAttributeValues'][':user'] == 'admin-user'

    def test_transaction_conditional_check_failure(self, mock_config, mock_gateway):
        """Test transaction failure due to conditional check."""
        # Simulate conditional check failure
        conditional_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'TransactionCanceledException',
                    'Message': 'Transaction cancelled due to conditional check failure',
                    'CancellationReasons': [
                        {'Code': 'ConditionalCheckFailed', 'Message': 'Pipeline does not exist'},
                        {'Code': 'None'},
                        {'Code': 'None'}
                    ]
                }
            },
            operation_name='TransactWriteItems'
        )
        
        mock_gateway.transact_write_items.side_effect = conditional_error
        
        # Mock the gateway to raise the mapped error directly
        mock_gateway.transact_write_items.side_effect = ConflictError("Transaction failed - conditional check", "pipeline-2")
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Transaction should fail atomically
            with pytest.raises(ConflictError, match="Transaction failed"):
                api.activate_pipelines(["pipeline-1", "pipeline-2", "pipeline-3"], "test-user")
            
            # Should attempt transaction once
            mock_gateway.transact_write_items.assert_called_once()

    def test_transaction_conflict_exception(self, mock_config, mock_gateway):
        """Test transaction failure due to conflict."""
        conflict_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'TransactionConflictException',
                    'Message': 'Transaction request cannot be processed due to conflict'
                }
            },
            operation_name='TransactWriteItems'
        )
        
        mock_gateway.transact_write_items.side_effect = conflict_error
        
        # Mock the gateway to raise the mapped error directly
        mock_gateway.transact_write_items.side_effect = ConflictError("Transaction conflict detected", "transaction-123")
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ConflictError, match="Transaction conflict"):
                api.activate_pipelines(["pipeline-1"], "test-user")

    def test_empty_pipeline_list_handling(self, mock_config, mock_gateway):
        """Test handling of empty pipeline list (no transaction needed)."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Empty list should return 0 without calling DynamoDB
            result = api.activate_pipelines([], "test-user")
            
            assert result == 0
            mock_gateway.transact_write_items.assert_not_called()


class TestTransactionCancellationReasons:
    """Test specific transaction cancellation scenarios."""

    def test_transaction_cancelled_with_multiple_failures(self, mock_config, mock_gateway):
        """Test transaction with multiple conditional check failures."""
        cancellation_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'TransactionCanceledException',
                    'Message': 'Transaction cancelled',
                    'CancellationReasons': [
                        {'Code': 'ConditionalCheckFailed', 'Message': 'Pipeline pipeline-1 does not exist'},
                        {'Code': 'None'},  # Second item succeeded
                        {'Code': 'ConditionalCheckFailed', 'Message': 'Pipeline pipeline-3 does not exist'}
                    ]
                }
            },
            operation_name='TransactWriteItems'
        )
        
        mock_gateway.transact_write_items.side_effect = cancellation_error
        
        # Mock the gateway to raise the mapped error directly
        mock_gateway.transact_write_items.side_effect = ConflictError("Multiple conditional checks failed", None)
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ConflictError):
                api.activate_pipelines(["pipeline-1", "pipeline-2", "pipeline-3"], "test-user")

    def test_transaction_validation_error(self, mock_config, mock_gateway):
        """Test transaction failure due to validation error."""
        validation_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ValidationException',
                    'Message': 'Invalid UpdateExpression syntax'
                }
            },
            operation_name='TransactWriteItems'
        )
        
        mock_gateway.transact_write_items.side_effect = validation_error
        
        # Mock the gateway to raise the mapped error directly
        mock_gateway.transact_write_items.side_effect = ValidationError("Invalid transaction syntax")
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ValidationError, match="Invalid transaction syntax"):
                api.activate_pipelines(["pipeline-1"], "test-user")

    def test_transaction_throttled(self, mock_config, mock_gateway):
        """Test transaction failure due to throttling."""
        throttling_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ProvisionedThroughputExceededException',
                    'Message': 'Request rate is too high for the transaction'
                }
            },
            operation_name='TransactWriteItems'
        )
        
        mock_gateway.transact_write_items.side_effect = throttling_error
        
        # Mock the gateway to raise the mapped error directly
        mock_gateway.transact_write_items.side_effect = RetryableError("Transaction throttled")
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(RetryableError, match="Transaction throttled"):
                api.activate_pipelines(["pipeline-1"], "test-user")


class TestTransactionItemConstruction:
    """Test proper construction of transaction items."""

    def test_activate_transaction_item_structure(self, mock_config, mock_gateway):
        """Test structure of transaction items for activation."""
        mock_gateway.transact_write_items.return_value = None
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            api.activate_pipelines(["test-pipeline"], "activate-user")
            
            # Examine the transaction item structure
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            transaction_item = call_args[0]
            
            assert 'Update' in transaction_item
            update = transaction_item['Update']
            
            # Verify required fields
            assert update['TableName'] == 'test_dev_pipeline_config'
            assert update['Key'] == {'pipeline_id': 'test-pipeline'}
            assert 'SET is_active = :active' in update['UpdateExpression']
            assert 'updated_at = :time' in update['UpdateExpression']
            assert 'updated_by = :user' in update['UpdateExpression']
            
            # Verify expression values
            values = update['ExpressionAttributeValues']
            assert values[':active'] == 'true'
            assert values[':user'] == 'activate-user'
            assert ':time' in values  # Timestamp should be present
            
            # Verify condition
            assert update['ConditionExpression'] == 'attribute_exists(pipeline_id)'

    def test_deactivate_transaction_item_structure(self, mock_config, mock_gateway):
        """Test structure of transaction items for deactivation."""
        mock_gateway.transact_write_items.return_value = None
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            api.deactivate_pipelines(["test-pipeline"], "deactivate-user")
            
            # Examine transaction item
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            transaction_item = call_args[0]
            update = transaction_item['Update']
            
            # Should set is_active to false
            assert update['ExpressionAttributeValues'][':active'] == 'false'
            assert update['ExpressionAttributeValues'][':user'] == 'deactivate-user'

    def test_transaction_without_updated_by(self, mock_config, mock_gateway):
        """Test transaction items when updated_by is not provided."""
        mock_gateway.transact_write_items.return_value = None
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Call without updated_by parameter
            api.activate_pipelines(["test-pipeline"])
            
            # Examine transaction item
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            transaction_item = call_args[0]
            update = transaction_item['Update']
            
            # Should not include updated_by in expression
            assert 'updated_by' not in update['UpdateExpression']
            assert ':user' not in update.get('ExpressionAttributeValues', {})
            
            # But should still have is_active and updated_at
            assert 'is_active = :active' in update['UpdateExpression']
            assert 'updated_at = :time' in update['UpdateExpression']


class TestTransactionLimits:
    """Test transaction limits and edge cases."""

    def test_transaction_with_maximum_items(self, mock_config, mock_gateway):
        """Test transaction with maximum number of items (100)."""
        mock_gateway.transact_write_items.return_value = None
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # DynamoDB supports up to 100 items in a transaction
            max_pipeline_ids = [f"pipeline-{i:03d}" for i in range(100)]
            
            result = api.activate_pipelines(max_pipeline_ids, "batch-user")
            
            assert result == 100
            mock_gateway.transact_write_items.assert_called_once()
            
            # Verify all items are in the transaction
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            assert len(call_args) == 100

    def test_transaction_exceeding_maximum_items(self, mock_config, mock_gateway):
        """Test behavior with more than 100 items (DynamoDB limit)."""
        # Note: Current implementation doesn't chunk transactions
        # This test documents the current behavior
        mock_gateway.transact_write_items.return_value = None
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # More than DynamoDB transaction limit
            too_many_ids = [f"pipeline-{i:03d}" for i in range(150)]
            
            # Current implementation will attempt all in one transaction
            # In real scenario, this would fail with ValidationException
            result = api.activate_pipelines(too_many_ids, "batch-user")
            
            assert result == 150
            
            # Verify it tries to put all in one transaction
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            assert len(call_args) == 150


class TestTransactionTimestampConsistency:
    """Test timestamp consistency within transactions."""

    def test_consistent_timestamps_across_transaction_items(self, mock_config, mock_gateway):
        """Test that all items in a transaction get the same timestamp."""
        mock_gateway.transact_write_items.return_value = None
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            api.activate_pipelines(["pipeline-1", "pipeline-2", "pipeline-3"], "timestamp-user")
            
            # Extract timestamps from all transaction items
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            timestamps = []
            
            for transaction_item in call_args:
                update = transaction_item['Update']
                timestamp = update['ExpressionAttributeValues'][':time']
                timestamps.append(timestamp)
            
            # All timestamps should be identical (same transaction time)
            assert len(set(timestamps)) == 1  # All timestamps are the same
            
            # Timestamp should be ISO format
            timestamp = timestamps[0]
            assert isinstance(timestamp, str)
            assert 'T' in timestamp  # ISO format should contain 'T'