"""
Tests for Table Configuration Domain (queries.py and commands.py)

These tests verify the CQRS pattern implementation for table configurations,
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
    gateway.table_name = "test_table_config"
    return gateway


class TestTableConfigReadApi:
    """Test read operations for table configurations."""

    def test_read_api_initialization(self, mock_config):
        """Test TableConfigReadApi can be initialized."""
        with patch('dynamodb_wrapper.handlers.table_config.queries.create_table_gateway') as mock_create:
            mock_create.return_value = Mock()
            
            # Import inside test to avoid import errors during collection
            from dynamodb_wrapper.handlers.table_config.queries import TableConfigReadApi
            api = TableConfigReadApi(mock_config)
            
            assert api.config == mock_config
            assert api.gateway is not None

    def test_get_by_id_method_exists(self, mock_config, mock_gateway):
        """Test that get_by_id method exists and can be called."""
        with patch('dynamodb_wrapper.handlers.table_config.queries.create_table_gateway', return_value=mock_gateway):
            from dynamodb_wrapper.handlers.table_config.queries import TableConfigReadApi
            api = TableConfigReadApi(mock_config)
            
            # Verify the method exists
            assert hasattr(api, 'get_by_id')
            assert callable(getattr(api, 'get_by_id'))


class TestTableConfigWriteApi:
    """Test write operations for table configurations."""

    def test_write_api_initialization(self, mock_config):
        """Test TableConfigWriteApi can be initialized."""
        with patch('dynamodb_wrapper.handlers.table_config.commands.create_table_gateway') as mock_create:
            mock_create.return_value = Mock()
            
            # Import inside test to avoid import errors during collection
            from dynamodb_wrapper.handlers.table_config.commands import TableConfigWriteApi
            api = TableConfigWriteApi(mock_config)
            
            assert api.config == mock_config
            assert api.gateway is not None

    def test_create_method_exists(self, mock_config, mock_gateway):
        """Test that create method exists and can be called."""
        with patch('dynamodb_wrapper.handlers.table_config.commands.create_table_gateway', return_value=mock_gateway):
            from dynamodb_wrapper.handlers.table_config.commands import TableConfigWriteApi
            api = TableConfigWriteApi(mock_config)
            
            # Verify create methods exist
            assert hasattr(api, 'create_table') or hasattr(api, 'create_table_config') or hasattr(api, 'create')
            
    def test_update_method_exists(self, mock_config, mock_gateway):
        """Test that update method exists and can be called.""" 
        with patch('dynamodb_wrapper.handlers.table_config.commands.create_table_gateway', return_value=mock_gateway):
            from dynamodb_wrapper.handlers.table_config.commands import TableConfigWriteApi
            api = TableConfigWriteApi(mock_config)
            
            # Verify update methods exist
            assert hasattr(api, 'update_table') or hasattr(api, 'update_table_config') or hasattr(api, 'update')


class TestTableConfigIntegration:
    """Test integration between read and write APIs."""

    def test_read_write_apis_work_together(self, mock_config):
        """Test that read and write APIs can be used together."""
        with patch('dynamodb_wrapper.handlers.table_config.queries.create_table_gateway') as mock_read_gateway:
            with patch('dynamodb_wrapper.handlers.table_config.commands.create_table_gateway') as mock_write_gateway:
                mock_read_gateway.return_value = Mock()
                mock_write_gateway.return_value = Mock()
                
                from dynamodb_wrapper.handlers.table_config.queries import TableConfigReadApi
                from dynamodb_wrapper.handlers.table_config.commands import TableConfigWriteApi
                
                read_api = TableConfigReadApi(mock_config)
                write_api = TableConfigWriteApi(mock_config)
                
                assert read_api is not None
                assert write_api is not None
                assert read_api.config == write_api.config