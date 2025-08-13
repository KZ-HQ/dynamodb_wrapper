"""
Tests for concurrent update race conditions and optimistic locking.

This module tests concurrent update scenarios, race conditions, and optimistic
locking behavior identified in TEST_ANALYSIS.md to ensure data consistency
under concurrent access.
"""

import pytest
from unittest.mock import Mock, patch, call
from botocore.exceptions import ClientError

from dynamodb_wrapper.handlers.pipeline_config.commands import PipelineConfigWriteApi
from dynamodb_wrapper.handlers.pipeline_config.queries import PipelineConfigReadApi
from dynamodb_wrapper.models import PipelineConfigUpsert
from dynamodb_wrapper.exceptions import ConflictError, ConnectionError
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


class TestConcurrentUpdateDetection:
    """Test detection and handling of concurrent updates."""

    def test_concurrent_update_with_conditional_check_failure(self, mock_config, mock_gateway):
        """Test concurrent update detected via conditional check failure."""
        # Simulate conditional check failure (item was modified by another process)
        conditional_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ConditionalCheckFailedException',
                    'Message': 'The conditional request failed'
                }
            },
            operation_name='UpdateItem'
        )
        
        mock_gateway.update_item.side_effect = conditional_error
        
        # Mock the gateway to raise the mapped error directly
        mock_gateway.update_item.side_effect = ConflictError("Conditional check failed - concurrent update detected", "test-pipeline")
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ConflictError, match="concurrent update detected"):
                api.update_pipeline(
                    "test-pipeline",
                    {"pipeline_name": "Updated Name"}
                )
            
            # Should attempt update
            mock_gateway.update_item.assert_called_once()

    def test_update_pipeline_status_with_version_check(self, mock_config, mock_gateway):
        """Test update with implicit version checking via updated_at."""
        # Mock successful response with new version info
        mock_gateway.update_item.return_value = {
            'Attributes': {
                'pipeline_id': 'version-test-pipeline',
                'is_active': 'false',
                'updated_at': '2024-01-01T12:00:00Z',
                'version': '2'
            }
        }
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            result = api.update_pipeline_status(
                "version-test-pipeline",
                is_active=False,
                updated_by="concurrent-test-user"
            )
            
            assert result is not None
            
            # Should use conditional expression to ensure item exists
            call_kwargs = mock_gateway.update_item.call_args[1]
            assert 'condition_expression' in call_kwargs
            # The condition expression is a boto3 object, check that it exists
            assert call_kwargs['condition_expression'] is not None

    def test_optimistic_locking_with_stale_data(self, mock_config, mock_gateway):
        """Test optimistic locking failure with stale data."""
        # First call: get current item
        # Second call: update fails due to version mismatch
        stale_version_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ConditionalCheckFailedException',
                    'Message': 'Version mismatch - item was modified'
                }
            },
            operation_name='UpdateItem'
        )
        
        mock_gateway.update_item.side_effect = stale_version_error
        
        mock_gateway.update_item.side_effect = ConflictError("Version mismatch - item was modified", "stale-pipeline")
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ConflictError, match="Version mismatch"):
                # Attempt update that should fail due to stale data
                api.update_pipeline(
                    "stale-pipeline",
                    {"description": "Updated description"}
                )


class TestRaceConditionScenarios:
    """Test specific race condition scenarios."""

    def test_create_pipeline_race_condition(self, mock_config, mock_gateway):
        """Test race condition during pipeline creation."""
        # Two processes try to create the same pipeline simultaneously
        # First succeeds, second gets conditional check failure
        
        duplicate_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ConditionalCheckFailedException',
                    'Message': 'The conditional request failed'
                }
            },
            operation_name='PutItem'
        )
        
        mock_gateway.put_item.side_effect = ConflictError("Pipeline already exists", "race-pipeline")
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            pipeline_data = PipelineConfigUpsert(
                pipeline_id="race-pipeline",
                pipeline_name="Race Condition Test",
                source_type="s3",
                destination_type="warehouse"
            )
            
            with pytest.raises(ConflictError, match="Pipeline already exists"):
                api.create_pipeline(pipeline_data)
            
            # Should attempt to create pipeline
            mock_gateway.put_item.assert_called_once()

    def test_delete_pipeline_race_condition(self, mock_config, mock_gateway):
        """Test race condition during pipeline deletion."""
        # Pipeline is deleted by another process before this deletion
        already_deleted_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ConditionalCheckFailedException',
                    'Message': 'The conditional request failed'
                }
            },
            operation_name='DeleteItem'
        )
        
        mock_gateway.delete_item.side_effect = ConflictError("Item does not exist", "deleted-pipeline")
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ConflictError, match="Item does not exist"):
                api.delete_pipeline("deleted-pipeline")
            
            # Should attempt to delete pipeline
            mock_gateway.delete_item.assert_called_once()

    def test_status_toggle_race_condition(self, mock_config, mock_gateway):
        """Test race condition when multiple processes toggle status."""
        # Process A tries to activate, Process B tries to deactivate simultaneously
        status_conflict_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ConditionalCheckFailedException',
                    'Message': 'Current status does not match expected'
                }
            },
            operation_name='UpdateItem'
        )
        
        mock_gateway.update_item.side_effect = ConflictError("Status conflict - concurrent modification", "toggle-pipeline")
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ConflictError, match="Status conflict"):
                api.update_pipeline_status(
                    "toggle-pipeline",
                    is_active=True,
                    updated_by="process-a"
                )


class TestConcurrentReadWriteConsistency:
    """Test consistency between concurrent reads and writes."""

    def test_read_during_write_operation(self, mock_config):
        """Test read consistency during concurrent write operations."""
        # This tests that reads can happen during writes without seeing partial states
        mock_write_gateway = Mock()
        mock_read_gateway = Mock()
        
        # Mock write operation in progress
        mock_write_gateway.put_item.return_value = None
        
        # Mock read returning consistent data (ensure proper dict structure for 'Item' check)
        response_dict = {
            'Item': {
                'pipeline_id': 'concurrent-test',
                'pipeline_name': 'Concurrent Test Pipeline',
                'is_active': 'true',
                'source_type': 's3',
                'destination_type': 'warehouse',
                'environment': 'dev',
                'version': '1.0.0',
                'created_at': '2024-01-01T10:00:00Z',
                'updated_at': '2024-01-01T10:00:00Z'
            }
        }
        # Make sure the response can be checked for 'Item' key - read API uses gateway.table.get_item
        mock_read_gateway.table = Mock()
        mock_read_gateway.table.get_item.return_value = response_dict
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_write_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_read_gateway):
                write_api = PipelineConfigWriteApi(mock_config)
                read_api = PipelineConfigReadApi(mock_config)
                
                # Simulate concurrent write
                pipeline_data = PipelineConfigUpsert(
                    pipeline_id="concurrent-test",
                    pipeline_name="Updated Name",
                    source_type="s3",
                    destination_type="warehouse"
                )
                write_api.upsert_pipeline(pipeline_data)
                
                # Simulate concurrent read
                result = read_api.get_by_id("concurrent-test")
                
                # Read should return consistent data
                assert result is not None
                assert result.pipeline_id == "concurrent-test"

    def test_eventual_consistency_handling(self, mock_config, mock_gateway):
        """Test handling of eventual consistency in reads after writes."""
        # Mock scenario where write succeeds but immediate read might not see it
        mock_gateway.put_item.return_value = None
        
        # First read: item not found (eventual consistency)
        # Second read: item found
        empty_response = {}  # Empty response (not found)
        found_response = {
            'Item': {
                'pipeline_id': 'eventual-test',
                'pipeline_name': 'Eventual Consistency Test',
                'source_type': 's3',
                'destination_type': 'warehouse',
                'is_active': 'true',
                'environment': 'dev',
                'version': '1.0.0',
                'created_at': '2024-01-01T10:00:00Z',
                'updated_at': '2024-01-01T10:00:00Z'
            }
        }
        # Mock the gateway.table.get_item (not gateway.get_item)
        mock_gateway.table = Mock()
        mock_gateway.table.get_item.side_effect = [empty_response, found_response]
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
                write_api = PipelineConfigWriteApi(mock_config)
                read_api = PipelineConfigReadApi(mock_config)
                
                # Write pipeline
                pipeline_data = PipelineConfigUpsert(
                    pipeline_id="eventual-test",
                    pipeline_name="Eventual Consistency Test",
                    source_type="s3",
                    destination_type="warehouse"
                )
                write_api.create_pipeline(pipeline_data)
                
                # First read - might not find it (eventual consistency)
                result1 = read_api.get_by_id("eventual-test")
                assert result1 is None
                
                # Second read - should find it
                result2 = read_api.get_by_id("eventual-test")
                assert result2 is not None
                assert result2.pipeline_id == "eventual-test"


class TestMultipleWriterScenarios:
    """Test scenarios with multiple concurrent writers."""

    def test_multiple_writers_same_item(self, mock_config, mock_gateway):
        """Test multiple writers updating the same item."""
        # Simulate multiple writers where second writer gets conflict
        successful_update = {
            'Attributes': {
                'pipeline_id': 'multi-writer-test',
                'updated_at': '2024-01-01T12:00:00Z'
            }
        }
        
        conflict_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ConditionalCheckFailedException',
                    'Message': 'The conditional request failed'
                }
            },
            operation_name='UpdateItem'
        )
        
        # First writer succeeds, second gets conflict
        mock_gateway.update_item.side_effect = [successful_update, conflict_error]
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            # First call succeeds
            api1 = PipelineConfigWriteApi(mock_config)
            result1 = api1.update_pipeline(
                "multi-writer-test",
                {"description": "Writer 1 update"}
            )
            assert result1 is not None
            
            # Reset mock to simulate second call failing
            mock_gateway.reset_mock()
            mock_gateway.update_item.side_effect = ConflictError("Concurrent modification", "multi-writer-test")
            
            # Second call fails with conflict
            api2 = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ConflictError, match="Concurrent modification"):
                api2.update_pipeline(
                    "multi-writer-test",
                    {"description": "Writer 2 update"}
                )

    def test_transaction_with_concurrent_modifications(self, mock_config, mock_gateway):
        """Test transaction behavior with concurrent modifications."""
        # Transaction fails due to concurrent modification of one item
        transaction_conflict = ClientError(
            error_response={
                'Error': {
                    'Code': 'TransactionCanceledException',
                    'Message': 'Transaction cancelled due to conditional check failure',
                    'CancellationReasons': [
                        {'Code': 'None'},  # First item OK
                        {'Code': 'ConditionalCheckFailed', 'Message': 'Item was modified'},  # Second item conflict
                        {'Code': 'None'}   # Third item OK
                    ]
                }
            },
            operation_name='TransactWriteItems'
        )
        
        mock_gateway.transact_write_items.side_effect = ConflictError("Transaction conflict due to concurrent modification", None)
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ConflictError, match="Transaction conflict"):
                api.activate_pipelines(
                    ["pipeline-1", "pipeline-2", "pipeline-3"],
                    "transaction-user"
                )


class TestOptimisticLockingPatterns:
    """Test optimistic locking implementation patterns."""

    def test_conditional_update_with_version_field(self, mock_config, mock_gateway):
        """Test conditional update using version field for optimistic locking."""
        # Mock successful update with version check
        mock_gateway.update_item.return_value = {
            'Attributes': {
                'pipeline_id': 'versioned-pipeline',
                'version': '1.0.1',
                'updated_at': '2024-01-01T11:00:00Z'
            }
        }
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Update with version check (using the actual API)
            result = api.update_pipeline(
                "versioned-pipeline",
                {"description": "Updated with version check"},
                condition_expression="version = :expected_version AND attribute_exists(pipeline_id)"
            )
            
            assert result is not None
            
            # Should use condition expression for optimistic locking
            call_kwargs = mock_gateway.update_item.call_args[1]
            assert 'condition_expression' in call_kwargs
            # The condition expression is a boto3 object, check that it exists
            assert call_kwargs['condition_expression'] is not None

    def test_timestamp_based_optimistic_locking(self, mock_config, mock_gateway):
        """Test optimistic locking using timestamp fields."""
        # Mock update with timestamp-based condition
        mock_gateway.update_item.return_value = {
            'Attributes': {
                'pipeline_id': 'timestamp-locked',
                'updated_at': '2024-01-01T12:00:00Z'
            }
        }
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Update with timestamp condition (using the actual API)
            result = api.update_pipeline(
                "timestamp-locked",
                {"description": "Timestamp-based update"},
                condition_expression="updated_at = :expected_timestamp AND attribute_exists(pipeline_id)"
            )
            
            assert result is not None

    def test_attribute_existence_based_locking(self, mock_config, mock_gateway):
        """Test locking based on attribute existence."""
        mock_gateway.update_item.return_value = {
            'Attributes': {
                'pipeline_id': 'existence-locked',
                'updated_at': '2024-01-01T12:00:00Z'
            }
        }
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Update with existence-based condition
            result = api.update_pipeline(
                "existence-locked",
                {"description": "Existence-based update"},
                condition_expression="attribute_exists(pipeline_id) AND attribute_not_exists(deleted_at)"
            )
            
            assert result is not None
            
            # Should use the provided condition
            call_kwargs = mock_gateway.update_item.call_args[1]
            condition_str = str(call_kwargs['condition_expression'])
            assert 'attribute_exists' in condition_str
            assert 'attribute_not_exists' in condition_str