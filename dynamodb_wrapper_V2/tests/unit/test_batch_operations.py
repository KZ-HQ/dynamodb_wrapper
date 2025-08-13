"""
Tests for batch operations including UnprocessedItems retry logic and size validation.

This module tests the critical gaps identified in TEST_ANALYSIS.md:
- UnprocessedItems retry logic with exponential backoff
- 400KB item size validation 
- Batch chunking (25 item DynamoDB limit)
- Throttling and error handling
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from dynamodb_wrapper.handlers.pipeline_config.commands import PipelineConfigWriteApi
from dynamodb_wrapper.models import PipelineConfigUpsert, PipelineConfig
from dynamodb_wrapper.exceptions import ValidationError, ConnectionError
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
    gateway.dynamodb = Mock()
    # Ensure table_name is properly set for string operations
    type(gateway).table_name = Mock(return_value="test_dev_pipeline_config")
    gateway.table_name = "test_dev_pipeline_config"
    return gateway


class TestBatchOperationsRetryLogic:
    """Test batch operations with UnprocessedItems retry logic."""

    def test_batch_write_success_no_retries(self, mock_config, mock_gateway):
        """Test successful batch write with no UnprocessedItems."""
        # Mock successful response with no unprocessed items
        mock_gateway.dynamodb.batch_write_item.return_value = {
            'UnprocessedItems': {}
        }
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                mock_convert.side_effect = lambda p: {'pipeline_id': p.pipeline_id, 'pipeline_name': p.pipeline_name}
                
                api = PipelineConfigWriteApi(mock_config)
                pipelines_data = [
                    PipelineConfigUpsert(
                        pipeline_id=f"success-{i}",
                        pipeline_name=f"Success Pipeline {i}",
                        source_type="s3",
                        destination_type="warehouse"
                    )
                    for i in range(3)
                ]
                
                result = api.upsert_many(pipelines_data)
                
                # Should succeed with all items
                assert len(result) == 3
                assert all(isinstance(p, PipelineConfig) for p in result)
                
                # Should call batch_write_item once
                mock_gateway.dynamodb.batch_write_item.assert_called_once()

    def test_batch_write_with_unprocessed_items_retry(self, mock_config, mock_gateway):
        """Test batch write with UnprocessedItems requiring retry."""
        # First call returns unprocessed items, second call succeeds
        mock_gateway.dynamodb.batch_write_item.side_effect = [
            {
                'UnprocessedItems': {
                    'test_dev_pipeline_config': [
                        {
                            'PutRequest': {
                                'Item': {'pipeline_id': 'retry-1', 'pipeline_name': 'Retry Pipeline 1'}
                            }
                        }
                    ]
                }
            },
            {
                'UnprocessedItems': {}
            }
        ]
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                with patch('time.sleep') as mock_sleep:  # Mock sleep to speed up tests
                    mock_convert.side_effect = lambda p: {'pipeline_id': p.pipeline_id, 'pipeline_name': p.pipeline_name}
                    
                    api = PipelineConfigWriteApi(mock_config)
                    pipelines_data = [
                        PipelineConfigUpsert(
                            pipeline_id="success-1",
                            pipeline_name="Success Pipeline 1",
                            source_type="s3",
                            destination_type="warehouse"
                        ),
                        PipelineConfigUpsert(
                            pipeline_id="retry-1",
                            pipeline_name="Retry Pipeline 1",
                            source_type="s3",
                            destination_type="warehouse"
                        )
                    ]
                    
                    result = api.upsert_many(pipelines_data)
                    
                    # Should succeed with all items after retry
                    assert len(result) == 2
                    
                    # Should call batch_write_item twice (initial + retry)
                    assert mock_gateway.dynamodb.batch_write_item.call_count == 2
                    
                    # Should have slept for backoff
                    mock_sleep.assert_called()

    def test_batch_write_max_retries_exceeded(self, mock_config, mock_gateway):
        """Test batch write failure after max retries."""
        # Always return unprocessed items
        mock_gateway.dynamodb.batch_write_item.return_value = {
            'UnprocessedItems': {
                'test_dev_pipeline_config': [
                    {
                        'PutRequest': {
                            'Item': {'pipeline_id': 'fail-1', 'pipeline_name': 'Fail Pipeline 1'}
                        }
                    }
                ]
            }
        }
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                with patch('time.sleep'):  # Mock sleep to speed up tests
                    mock_convert.side_effect = lambda p: {'pipeline_id': p.pipeline_id, 'pipeline_name': p.pipeline_name}
                    
                    api = PipelineConfigWriteApi(mock_config)
                    pipelines_data = [
                        PipelineConfigUpsert(
                            pipeline_id="fail-1",
                            pipeline_name="Fail Pipeline 1",
                            source_type="s3",
                            destination_type="warehouse"
                        )
                    ]
                    
                    with pytest.raises(ConnectionError, match="Batch write failed for 1 items after 3 retries"):
                        api.upsert_many(pipelines_data)
                        
                    # Should retry max_retries + 1 times (4 total)
                    assert mock_gateway.dynamodb.batch_write_item.call_count == 4

    def test_batch_write_throttling_retry(self, mock_config, mock_gateway):
        """Test batch write with throttling exception retry."""
        # First call throws throttling, second succeeds
        throttling_error = ClientError(
            error_response={
                'Error': {
                    'Code': 'ProvisionedThroughputExceededException',
                    'Message': 'Request rate is too high'
                }
            },
            operation_name='BatchWriteItem'
        )
        
        mock_gateway.dynamodb.batch_write_item.side_effect = [
            throttling_error,
            {'UnprocessedItems': {}}
        ]
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                with patch('time.sleep') as mock_sleep:
                    mock_convert.side_effect = lambda p: {'pipeline_id': p.pipeline_id, 'pipeline_name': p.pipeline_name}
                    
                    api = PipelineConfigWriteApi(mock_config)
                    pipelines_data = [
                        PipelineConfigUpsert(
                            pipeline_id="throttle-test",
                            pipeline_name="Throttle Test",
                            source_type="s3",
                            destination_type="warehouse"
                        )
                    ]
                    
                    result = api.upsert_many(pipelines_data)
                    
                    # Should succeed after retry
                    assert len(result) == 1
                    
                    # Should retry once
                    assert mock_gateway.dynamodb.batch_write_item.call_count == 2
                    
                    # Should backoff for throttling (longer delay)
                    mock_sleep.assert_called()


class TestItemSizeValidation:
    """Test 400KB item size validation."""

    def test_item_size_calculation(self, mock_config, mock_gateway):
        """Test item size calculation accuracy."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Small item
            small_item = {'pipeline_id': 'small', 'pipeline_name': 'Small Pipeline'}
            small_size = api._calculate_item_size(small_item)
            assert small_size < 1000  # Should be under 1KB
            
            # Large item (approximately 1KB)
            large_item = {
                'pipeline_id': 'large',
                'pipeline_name': 'Large Pipeline',
                'description': 'x' * 1000  # 1KB string
            }
            large_size = api._calculate_item_size(large_item)
            assert 1000 < large_size < 1200  # Should be around 1KB

    def test_item_size_limit_enforcement(self, mock_config, mock_gateway):
        """Test that items exceeding 400KB are rejected."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Create an item that exceeds 400KB
            oversized_pipeline = PipelineConfig(
                pipeline_id="oversized",
                pipeline_name="Oversized Pipeline",
                description="x" * (450 * 1024),  # 450KB string
                source_type="s3",
                destination_type="warehouse"
            )
            
            with pytest.raises(ValidationError, match="exceeds 400KB DynamoDB limit"):
                api._batch_write_with_retry([oversized_pipeline])

    def test_item_size_validation_multiple_items(self, mock_config, mock_gateway):
        """Test size validation with multiple items."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Mix of valid and invalid items
            pipelines = [
                PipelineConfig(
                    pipeline_id="valid-1",
                    pipeline_name="Valid Pipeline 1",
                    source_type="s3",
                    destination_type="warehouse"
                ),
                PipelineConfig(
                    pipeline_id="oversized",
                    pipeline_name="Oversized Pipeline",
                    description="x" * (450 * 1024),  # 450KB
                    source_type="s3",
                    destination_type="warehouse"
                ),
                PipelineConfig(
                    pipeline_id="valid-2",
                    pipeline_name="Valid Pipeline 2",
                    source_type="s3",
                    destination_type="warehouse"
                )
            ]
            
            # Should fail on the oversized item
            with pytest.raises(ValidationError, match="exceeds 400KB DynamoDB limit for pipeline oversized"):
                api._batch_write_with_retry(pipelines)


class TestBatchChunking:
    """Test batch operation chunking for DynamoDB limits."""

    def test_batch_chunking_25_item_limit(self, mock_config, mock_gateway):
        """Test that large batches are chunked to 25 items."""
        mock_gateway.dynamodb.batch_write_item.return_value = {'UnprocessedItems': {}}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                mock_convert.side_effect = lambda p: {'pipeline_id': p.pipeline_id, 'pipeline_name': p.pipeline_name}
                
                api = PipelineConfigWriteApi(mock_config)
                
                # Create 30 items (more than 25)
                pipelines_data = [
                    PipelineConfigUpsert(
                        pipeline_id=f"chunk-{i:03d}",
                        pipeline_name=f"Chunk Pipeline {i}",
                        source_type="s3",
                        destination_type="warehouse"
                    )
                    for i in range(30)
                ]
                
                result = api.upsert_many(pipelines_data)
                
                # Should process all items
                assert len(result) == 30
                
                # Should make 2 batch calls (25 + 5)
                assert mock_gateway.dynamodb.batch_write_item.call_count == 2
                
                # Verify chunk sizes
                calls = mock_gateway.dynamodb.batch_write_item.call_args_list
                
                # First chunk should have 25 items
                first_chunk = calls[0][1]['RequestItems']['test_dev_pipeline_config']
                assert len(first_chunk) == 25
                
                # Second chunk should have 5 items
                second_chunk = calls[1][1]['RequestItems']['test_dev_pipeline_config']
                assert len(second_chunk) == 5

    def test_batch_chunking_exactly_25_items(self, mock_config, mock_gateway):
        """Test batch with exactly 25 items (no chunking needed)."""
        mock_gateway.dynamodb.batch_write_item.return_value = {'UnprocessedItems': {}}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                mock_convert.side_effect = lambda p: {'pipeline_id': p.pipeline_id, 'pipeline_name': p.pipeline_name}
                
                api = PipelineConfigWriteApi(mock_config)
                
                # Create exactly 25 items
                pipelines_data = [
                    PipelineConfigUpsert(
                        pipeline_id=f"exact-{i:02d}",
                        pipeline_name=f"Exact Pipeline {i}",
                        source_type="s3",
                        destination_type="warehouse"
                    )
                    for i in range(25)
                ]
                
                result = api.upsert_many(pipelines_data)
                
                # Should process all items
                assert len(result) == 25
                
                # Should make exactly 1 batch call
                assert mock_gateway.dynamodb.batch_write_item.call_count == 1


class TestPartialFailureHandling:
    """Test partial failure scenarios in batch operations."""

    def test_partial_success_with_unprocessed_items(self, mock_config, mock_gateway):
        """Test handling of partial success with some unprocessed items."""
        # Simulate partial failure - some items succeed, others need retry
        mock_responses = [
            {
                'UnprocessedItems': {
                    'test_dev_pipeline_config': [
                        {
                            'PutRequest': {
                                'Item': {'pipeline_id': 'retry-item', 'pipeline_name': 'Retry Item'}
                            }
                        }
                    ]
                }
            },
            {
                'UnprocessedItems': {}
            }
        ]
        mock_gateway.dynamodb.batch_write_item.side_effect = mock_responses
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                with patch('time.sleep'):
                    mock_convert.side_effect = lambda p: {'pipeline_id': p.pipeline_id, 'pipeline_name': p.pipeline_name}
                    
                    api = PipelineConfigWriteApi(mock_config)
                    pipelines_data = [
                        PipelineConfigUpsert(
                            pipeline_id="success-item",
                            pipeline_name="Success Item",
                            source_type="s3",
                            destination_type="warehouse"
                        ),
                        PipelineConfigUpsert(
                            pipeline_id="retry-item",
                            pipeline_name="Retry Item",
                            source_type="s3",
                            destination_type="warehouse"
                        )
                    ]
                    
                    result = api.upsert_many(pipelines_data)
                    
                    # Should eventually succeed with all items
                    assert len(result) == 2
                    
                    # Should retry the unprocessed item
                    assert mock_gateway.dynamodb.batch_write_item.call_count == 2

    def test_empty_batch_handling(self, mock_config, mock_gateway):
        """Test handling of empty batch."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            result = api.upsert_many([])
            
            # Should return empty list
            assert result == []
            
            # Should not call DynamoDB
            mock_gateway.dynamodb.batch_write_item.assert_not_called()