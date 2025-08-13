"""
Tests for Pipeline Configuration Domain (queries.py and commands.py)

These tests verify the CQRS pattern implementation for pipeline configurations,
including read/write API separation, query optimization, and domain operations.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
from pydantic import ValidationError as PydanticValidationError

from dynamodb_wrapper.config import DynamoDBConfig
from dynamodb_wrapper.handlers.pipeline_config.queries import PipelineConfigReadApi
from dynamodb_wrapper.handlers.pipeline_config.commands import PipelineConfigWriteApi
from dynamodb_wrapper.models import PipelineConfigUpsert, PipelineConfig, PipelineConfigView
from dynamodb_wrapper.exceptions import ValidationError, ItemNotFoundError, ConflictError


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    return DynamoDBConfig(
        region_name="us-east-1",
        table_prefix="test",
        environment="dev"
    )


@pytest.fixture
def mock_gateway():
    """Mock table gateway for testing."""
    gateway = Mock()
    gateway.table = Mock()
    gateway.table_name = "test_pipeline_config"
    return gateway


class TestPipelineConfigReadApi:
    """Test read operations for pipeline configurations."""

    def test_initialization(self, mock_config):
        """Test PipelineConfigReadApi initialization."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway') as mock_create:
            mock_create.return_value = Mock()
            api = PipelineConfigReadApi(mock_config)
            
            assert api.config == mock_config
            assert api.gateway is not None
            assert len(api.default_projection) > 0
            mock_create.assert_called_once_with(mock_config, "pipeline_config")

    def test_get_by_id_found(self, mock_config, mock_gateway):
        """Test get_by_id when pipeline is found."""
        # Setup mock response
        mock_item = {
            'pipeline_id': 'test-pipeline',
            'pipeline_name': 'Test Pipeline',
            'source_type': 's3',
            'destination_type': 'warehouse',
            'is_active': True,
            'environment': 'dev',
            'created_at': '2024-01-01T10:00:00+00:00'
        }
        mock_gateway.table.get_item.return_value = {'Item': mock_item}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.item_to_model') as mock_convert:
                mock_view = Mock(spec=PipelineConfigView)
                mock_convert.return_value = mock_view
                
                api = PipelineConfigReadApi(mock_config)
                result = api.get_by_id('test-pipeline')
                
                assert result == mock_view
                mock_gateway.table.get_item.assert_called_once()
                mock_convert.assert_called_once_with(mock_item, PipelineConfigView)

    def test_get_by_id_not_found(self, mock_config, mock_gateway):
        """Test get_by_id when pipeline is not found."""
        mock_gateway.table.get_item.return_value = {}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigReadApi(mock_config)
            result = api.get_by_id('nonexistent-pipeline')
            
            assert result is None

    def test_get_by_id_with_projection(self, mock_config, mock_gateway):
        """Test get_by_id with custom projection."""
        mock_gateway.table.get_item.return_value = {'Item': {'pipeline_id': 'test'}}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.build_projection_expression') as mock_proj:
                mock_proj.return_value = ('#f0', {'#f0': 'pipeline_id'})
                with patch('dynamodb_wrapper.handlers.pipeline_config.queries.item_to_model'):
                    api = PipelineConfigReadApi(mock_config)
                    api.get_by_id('test-pipeline', projection=['pipeline_id'])
                    
                    mock_proj.assert_called_with(['pipeline_id'])
                    call_args = mock_gateway.table.get_item.call_args[1]
                    assert 'ProjectionExpression' in call_args
                    assert 'ExpressionAttributeNames' in call_args

    def test_query_active_pipelines(self, mock_config, mock_gateway):
        """Test querying active pipelines."""
        mock_items = [
            {'pipeline_id': 'pipeline-1', 'is_active': True},
            {'pipeline_id': 'pipeline-2', 'is_active': True}
        ]
        mock_gateway.query.return_value = {
            'Items': mock_items,
            'LastEvaluatedKey': {'pipeline_id': 'pipeline-2'}
        }
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.item_to_model') as mock_convert:
                mock_views = [Mock(spec=PipelineConfigView) for _ in mock_items]
                mock_convert.side_effect = mock_views
                
                api = PipelineConfigReadApi(mock_config)
                items, last_key = api.query_active_pipelines()
                
                assert len(items) == 2
                assert last_key == {'pipeline_id': 'pipeline-2'}
                
                # Verify query parameters
                call_args = mock_gateway.query.call_args[1]
                assert call_args['IndexName'] == 'ActivePipelinesIndex'
                assert call_args['ScanIndexForward'] is False

    def test_query_by_environment(self, mock_config, mock_gateway):
        """Test querying pipelines by environment."""
        mock_items = [{'pipeline_id': 'pipeline-1', 'environment': 'prod'}]
        mock_gateway.query.return_value = {'Items': mock_items}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.item_to_model') as mock_convert:
                mock_convert.return_value = Mock(spec=PipelineConfigView)
                
                api = PipelineConfigReadApi(mock_config)
                items, last_key = api.query_by_environment('prod')
                
                assert len(items) == 1
                assert last_key is None
                
                # Verify query parameters
                call_args = mock_gateway.query.call_args[1]
                assert call_args['IndexName'] == 'EnvironmentIndex'

    def test_query_by_environment_and_status(self, mock_config, mock_gateway):
        """Test querying pipelines by environment and status."""
        mock_items = [{'pipeline_id': 'pipeline-1'}]
        mock_gateway.query.return_value = {'Items': mock_items}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.item_to_model') as mock_convert:
                mock_convert.return_value = Mock(spec=PipelineConfigView)
                
                api = PipelineConfigReadApi(mock_config)
                items, last_key = api.query_by_environment_and_status('prod', True)
                
                assert len(items) == 1
                
                # Verify query has FilterExpression
                call_args = mock_gateway.query.call_args[1]
                assert 'FilterExpression' in call_args

    def test_scan_for_all_pipelines_with_warning(self, mock_config, mock_gateway):
        """Test scan operation includes warning for expensive operation."""
        mock_items = [{'pipeline_id': 'pipeline-1'}]
        mock_gateway.scan.return_value = {'Items': mock_items}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.item_to_model') as mock_convert:
                with patch('dynamodb_wrapper.handlers.pipeline_config.queries.logger') as mock_logger:
                    mock_convert.return_value = Mock(spec=PipelineConfigView)
                    
                    api = PipelineConfigReadApi(mock_config)
                    items, last_key = api.scan_for_all_pipelines()
                    
                    assert len(items) == 1
                    mock_logger.warning.assert_called_once()

    def test_get_pipeline_summary(self, mock_config, mock_gateway):
        """Test getting pipeline summary with minimal projection."""
        mock_item = {'pipeline_id': 'test', 'pipeline_name': 'Test'}
        mock_gateway.table.get_item.return_value = {'Item': mock_item}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.item_to_model') as mock_convert:
                from dynamodb_wrapper.models import PipelineConfigSummaryView
                mock_summary = Mock(spec=PipelineConfigSummaryView)
                mock_convert.return_value = mock_summary
                
                api = PipelineConfigReadApi(mock_config)
                result = api.get_pipeline_summary('test-pipeline')
                
                assert result == mock_summary
                mock_convert.assert_called_with(mock_item, PipelineConfigSummaryView)

    def test_count_pipelines_by_environment(self, mock_config, mock_gateway):
        """Test counting pipelines by environment."""
        mock_gateway.query.return_value = {'Count': 5}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigReadApi(mock_config)
            count = api.count_pipelines_by_environment('prod')
            
            assert count == 5
            
            # Verify query uses COUNT select
            call_args = mock_gateway.query.call_args[1]
            assert call_args['Select'] == 'COUNT'

    def test_count_active_pipelines(self, mock_config, mock_gateway):
        """Test counting active pipelines."""
        mock_gateway.query.return_value = {'Count': 10}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.queries.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigReadApi(mock_config)
            count = api.count_active_pipelines()
            
            assert count == 10


class TestPipelineConfigWriteApi:
    """Test write operations for pipeline configurations."""

    def test_initialization(self, mock_config):
        """Test PipelineConfigWriteApi initialization."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway') as mock_create:
            mock_create.return_value = Mock()
            api = PipelineConfigWriteApi(mock_config)
            
            assert api.config == mock_config
            assert api.gateway is not None
            mock_create.assert_called_once_with(mock_config, "pipeline_config")

    def test_create_pipeline_success(self, mock_config, mock_gateway):
        """Test successful pipeline creation."""
        pipeline_data = PipelineConfigUpsert(
            pipeline_id='test-pipeline',
            pipeline_name='Test Pipeline',
            source_type='s3',
            destination_type='warehouse'
        )
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                mock_convert.return_value = {'pipeline_id': 'test-pipeline'}
                
                api = PipelineConfigWriteApi(mock_config)
                result = api.create_pipeline(pipeline_data)
                
                assert isinstance(result, PipelineConfig)
                assert result.pipeline_id == 'test-pipeline'
                assert result.created_at is not None
                assert result.updated_at is not None
                
                mock_gateway.put_item.assert_called_once()
                call_args = mock_gateway.put_item.call_args
                assert 'condition_expression' in call_args[1]

    def test_create_pipeline_validation_error(self, mock_config, mock_gateway):
        """Test pipeline creation with validation error."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            # Test that Pydantic validation fails for invalid data
            with pytest.raises(PydanticValidationError):
                # Invalid data that would cause Pydantic validation to fail
                PipelineConfigUpsert(
                    pipeline_id='',  # Empty ID should cause validation error
                    pipeline_name='Test',
                    source_type='s3',
                    destination_type='warehouse'
                )

    def test_update_pipeline_success(self, mock_config, mock_gateway):
        """Test successful pipeline update."""
        mock_gateway.update_item.return_value = {'Attributes': {'pipeline_id': 'test'}}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            updates = {'pipeline_name': 'Updated Name', 'is_active': False}
            
            result = api.update_pipeline('test-pipeline', updates)
            
            assert result is not None
            mock_gateway.update_item.assert_called_once()
            
            # Verify update expression includes updated_at
            call_args = mock_gateway.update_item.call_args[1]
            assert 'update_expression' in call_args
            assert 'updated_at' in call_args['update_expression']

    def test_update_pipeline_empty_updates(self, mock_config, mock_gateway):
        """Test pipeline update with empty updates dictionary."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            with pytest.raises(ValidationError, match="Updates dictionary cannot be empty"):
                api.update_pipeline('test-pipeline', {})

    def test_update_pipeline_status(self, mock_config, mock_gateway):
        """Test updating pipeline status."""
        mock_gateway.update_item.return_value = {'Attributes': {'is_active': False}}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            result = api.update_pipeline_status('test-pipeline', False, 'test-user')
            
            assert result is not None
            mock_gateway.update_item.assert_called_once()

    def test_delete_pipeline_success(self, mock_config, mock_gateway):
        """Test successful pipeline deletion."""
        mock_gateway.delete_item.return_value = {'Attributes': {'pipeline_id': 'test'}}
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            result = api.delete_pipeline('test-pipeline')
            
            assert result is True
            mock_gateway.delete_item.assert_called_once()
            
            # Verify condition expression requires existence
            call_args = mock_gateway.delete_item.call_args[1]
            assert 'condition_expression' in call_args

    def test_upsert_pipeline(self, mock_config, mock_gateway):
        """Test pipeline upsert operation."""
        pipeline_data = PipelineConfigUpsert(
            pipeline_id='test-pipeline',
            pipeline_name='Test Pipeline',
            source_type='s3',
            destination_type='warehouse'
        )
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                mock_convert.return_value = {'pipeline_id': 'test-pipeline'}
                
                api = PipelineConfigWriteApi(mock_config)
                result = api.upsert_pipeline(pipeline_data)
                
                assert isinstance(result, PipelineConfig)
                mock_gateway.put_item.assert_called_once()
                
                # Verify no condition expression (allows overwrite)
                call_args = mock_gateway.put_item.call_args
                assert len(call_args[1]) == 0  # No kwargs means no condition

    def test_upsert_many(self, mock_config, mock_gateway):
        """Test bulk upsert operation."""
        pipelines_data = [
            PipelineConfigUpsert(
                pipeline_id=f'pipeline-{i}',
                pipeline_name=f'Pipeline {i}',
                source_type='s3',
                destination_type='warehouse'
            )
            for i in range(3)
        ]
        
        # Mock successful batch write response for new retry logic
        mock_gateway.dynamodb = Mock()
        mock_gateway.dynamodb.batch_write_item.return_value = {
            'UnprocessedItems': {}
        }
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.commands.model_to_item') as mock_convert:
                mock_convert.side_effect = lambda p: {'pipeline_id': p.pipeline_id, 'pipeline_name': p.pipeline_name}
                
                api = PipelineConfigWriteApi(mock_config)
                result = api.upsert_many(pipelines_data)
                
                assert len(result) == 3
                assert all(isinstance(p, PipelineConfig) for p in result)
                mock_gateway.dynamodb.batch_write_item.assert_called_once()

    def test_upsert_many_empty_list(self, mock_config, mock_gateway):
        """Test bulk upsert with empty list."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            result = api.upsert_many([])
            
            assert result == []

    def test_activate_pipelines_transaction(self, mock_config, mock_gateway):
        """Test activating multiple pipelines in transaction."""
        pipeline_ids = ['pipeline-1', 'pipeline-2', 'pipeline-3']
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            result = api.activate_pipelines(pipeline_ids, 'test-user')
            
            assert result == 3
            mock_gateway.transact_write_items.assert_called_once()
            
            # Verify transaction structure
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            assert len(call_args) == 3
            for item in call_args:
                assert 'Update' in item
                assert ':active' in item['Update']['ExpressionAttributeValues']
                assert item['Update']['ExpressionAttributeValues'][':active'] == 'true'

    def test_deactivate_pipelines_transaction(self, mock_config, mock_gateway):
        """Test deactivating multiple pipelines in transaction."""
        pipeline_ids = ['pipeline-1', 'pipeline-2']
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            result = api.deactivate_pipelines(pipeline_ids)
            
            assert result == 2
            mock_gateway.transact_write_items.assert_called_once()
            
            # Verify deactivation
            call_args = mock_gateway.transact_write_items.call_args[0][0]
            for item in call_args:
                assert item['Update']['ExpressionAttributeValues'][':active'] == 'false'

    def test_activate_pipelines_empty_list(self, mock_config, mock_gateway):
        """Test activating empty list of pipelines."""
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            api = PipelineConfigWriteApi(mock_config)
            
            result = api.activate_pipelines([])
            
            assert result == 0
            mock_gateway.transact_write_items.assert_not_called()

    def test_archive_old_pipelines_integration(self, mock_config, mock_gateway):
        """Test archiving old pipelines with read/write API integration."""
        # Mock read API results
        mock_read_api = Mock()
        mock_pipelines = [
            Mock(pipeline_id='old-pipeline-1'),
            Mock(pipeline_id='old-pipeline-2')
        ]
        mock_read_api.query_by_environment_and_status.return_value = (mock_pipelines, None)
        
        with patch('dynamodb_wrapper.handlers.pipeline_config.commands.create_table_gateway', return_value=mock_gateway):
            with patch('dynamodb_wrapper.handlers.pipeline_config.queries.PipelineConfigReadApi', return_value=mock_read_api):
                api = PipelineConfigWriteApi(mock_config)
                
                # Mock the update_pipeline method to succeed
                api.update_pipeline = Mock(return_value={'archived': True})
                
                result = api.archive_old_pipelines('prod', days_inactive=90, updated_by='system')
                
                assert result == 2
                assert api.update_pipeline.call_count == 2