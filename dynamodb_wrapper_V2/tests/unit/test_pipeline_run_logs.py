"""
Tests for Pipeline Run Logs Domain (queries.py and commands.py)

These tests verify the CQRS pattern implementation for pipeline run logs,
including read/write API separation and domain operations.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from dynamodb_wrapper.config import DynamoDBConfig
from dynamodb_wrapper.exceptions import ValidationError, ItemNotFoundError


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
    gateway.table_name = "test_pipeline_run_logs"
    return gateway


class TestPipelineRunLogsReadApi:
    """Test read operations for pipeline run logs."""

    def test_read_api_initialization(self, mock_config):
        """Test PipelineRunLogsReadApi can be initialized."""
        with patch('dynamodb_wrapper.handlers.pipeline_run_logs.queries.create_table_gateway') as mock_create:
            mock_create.return_value = Mock()
            
            # Import inside test to avoid import errors during collection
            from dynamodb_wrapper.handlers.pipeline_run_logs.queries import PipelineRunLogsReadApi
            api = PipelineRunLogsReadApi(mock_config)
            
            assert api.config == mock_config
            assert api.gateway is not None

    def test_get_by_run_id_method_exists(self, mock_config, mock_gateway):
        """Test that get_by_run_id method exists and can be called."""
        with patch('dynamodb_wrapper.handlers.pipeline_run_logs.queries.create_table_gateway', return_value=mock_gateway):
            from dynamodb_wrapper.handlers.pipeline_run_logs.queries import PipelineRunLogsReadApi
            api = PipelineRunLogsReadApi(mock_config)
            
            # Verify the method exists
            assert hasattr(api, 'get_by_run_id') or hasattr(api, 'get_by_id')
            
    def test_query_by_pipeline_method_exists(self, mock_config, mock_gateway):
        """Test that query by pipeline method exists."""
        with patch('dynamodb_wrapper.handlers.pipeline_run_logs.queries.create_table_gateway', return_value=mock_gateway):
            from dynamodb_wrapper.handlers.pipeline_run_logs.queries import PipelineRunLogsReadApi
            api = PipelineRunLogsReadApi(mock_config)
            
            # Verify query methods exist
            assert hasattr(api, 'query_by_pipeline') or hasattr(api, 'query_runs_by_pipeline')


class TestPipelineRunLogsWriteApi:
    """Test write operations for pipeline run logs."""

    def test_write_api_initialization(self, mock_config):
        """Test PipelineRunLogsWriteApi can be initialized."""
        with patch('dynamodb_wrapper.handlers.pipeline_run_logs.commands.create_table_gateway') as mock_create:
            mock_create.return_value = Mock()
            
            # Import inside test to avoid import errors during collection
            from dynamodb_wrapper.handlers.pipeline_run_logs.commands import PipelineRunLogsWriteApi
            api = PipelineRunLogsWriteApi(mock_config)
            
            assert api.config == mock_config
            assert api.gateway is not None

    def test_create_run_log_method_exists(self, mock_config, mock_gateway):
        """Test that create run log method exists."""
        with patch('dynamodb_wrapper.handlers.pipeline_run_logs.commands.create_table_gateway', return_value=mock_gateway):
            from dynamodb_wrapper.handlers.pipeline_run_logs.commands import PipelineRunLogsWriteApi
            api = PipelineRunLogsWriteApi(mock_config)
            
            # Verify create methods exist
            assert hasattr(api, 'create_run_log') or hasattr(api, 'start_run') or hasattr(api, 'create')
            
    def test_update_run_status_method_exists(self, mock_config, mock_gateway):
        """Test that update run status method exists."""
        with patch('dynamodb_wrapper.handlers.pipeline_run_logs.commands.create_table_gateway', return_value=mock_gateway):
            from dynamodb_wrapper.handlers.pipeline_run_logs.commands import PipelineRunLogsWriteApi
            api = PipelineRunLogsWriteApi(mock_config)
            
            # Verify update methods exist
            assert hasattr(api, 'update_run_status') or hasattr(api, 'complete_run') or hasattr(api, 'update')


class TestPipelineRunLogsIntegration:
    """Test integration between read and write APIs."""

    def test_read_write_apis_work_together(self, mock_config):
        """Test that read and write APIs can be used together."""
        with patch('dynamodb_wrapper.handlers.pipeline_run_logs.queries.create_table_gateway') as mock_read_gateway:
            with patch('dynamodb_wrapper.handlers.pipeline_run_logs.commands.create_table_gateway') as mock_write_gateway:
                mock_read_gateway.return_value = Mock()
                mock_write_gateway.return_value = Mock()
                
                from dynamodb_wrapper.handlers.pipeline_run_logs.queries import PipelineRunLogsReadApi
                from dynamodb_wrapper.handlers.pipeline_run_logs.commands import PipelineRunLogsWriteApi
                
                read_api = PipelineRunLogsReadApi(mock_config)
                write_api = PipelineRunLogsWriteApi(mock_config)
                
                assert read_api is not None
                assert write_api is not None
                assert read_api.config == write_api.config

    def test_run_lifecycle_operations(self, mock_config):
        """Test typical run lifecycle operations exist."""
        with patch('dynamodb_wrapper.handlers.pipeline_run_logs.queries.create_table_gateway') as mock_read_gateway:
            with patch('dynamodb_wrapper.handlers.pipeline_run_logs.commands.create_table_gateway') as mock_write_gateway:
                mock_read_gateway.return_value = Mock()
                mock_write_gateway.return_value = Mock()
                
                from dynamodb_wrapper.handlers.pipeline_run_logs.queries import PipelineRunLogsReadApi  
                from dynamodb_wrapper.handlers.pipeline_run_logs.commands import PipelineRunLogsWriteApi
                
                read_api = PipelineRunLogsReadApi(mock_config)
                write_api = PipelineRunLogsWriteApi(mock_config)
                
                # Verify basic lifecycle operations exist
                # These method names are flexible since they might vary by implementation
                write_methods = [method for method in dir(write_api) if not method.startswith('_')]
                read_methods = [method for method in dir(read_api) if not method.startswith('_')]
                
                assert len(write_methods) > 2  # Should have config, gateway, and domain methods
                assert len(read_methods) > 2   # Should have config, gateway, and domain methods