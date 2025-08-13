"""
Tests for TableGateway (core/table_gateway.py)

These tests verify the thin DynamoDB wrapper that provides building blocks
for CQRS read/write APIs without heavyweight abstractions.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError

from dynamodb_wrapper.config import DynamoDBConfig
from dynamodb_wrapper.core.table_gateway import TableGateway, create_table_gateway
from dynamodb_wrapper.exceptions import ConnectionError, ConflictError


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    return DynamoDBConfig(
        region_name="us-east-1",
        table_prefix="test",
        environment="dev",
        aws_access_key_id="fake_key",
        aws_secret_access_key="fake_secret"
    )


@pytest.fixture
def mock_session():
    """Mock boto3 session."""
    session = Mock()
    session.resource.return_value = Mock()
    return session


@pytest.fixture
def mock_table():
    """Mock DynamoDB table resource."""
    table = Mock()
    table.query.return_value = {'Items': []}
    table.scan.return_value = {'Items': []}
    table.put_item.return_value = None
    table.update_item.return_value = {'Attributes': {}}
    table.delete_item.return_value = {'Attributes': {}}
    return table


class TestTableGateway:
    """Test TableGateway class."""

    def test_initialization(self, mock_config):
        """Test TableGateway initialization."""
        gateway = TableGateway(mock_config, "test_table")
        
        assert gateway.config == mock_config
        assert gateway.table_name == "test_table"
        assert gateway._dynamodb is None
        assert gateway._table is None

    def test_dynamodb_property_lazy_initialization(self, mock_config):
        """Test lazy initialization of DynamoDB resource."""
        with patch('boto3.Session') as mock_session_class:
            mock_session = Mock()
            mock_dynamodb = Mock()
            mock_session_class.return_value = mock_session
            mock_session.resource.return_value = mock_dynamodb
            
            gateway = TableGateway(mock_config, "test_table")
            
            # First access should create the resource
            result = gateway.dynamodb
            
            assert result == mock_dynamodb
            assert gateway._dynamodb == mock_dynamodb
            mock_session_class.assert_called_once()
            mock_session.resource.assert_called_once()

    def test_dynamodb_property_reuses_instance(self, mock_config):
        """Test that DynamoDB resource is reused on subsequent accesses."""
        with patch('boto3.Session') as mock_session_class:
            mock_session = Mock()
            mock_dynamodb = Mock()
            mock_session_class.return_value = mock_session
            mock_session.resource.return_value = mock_dynamodb
            
            gateway = TableGateway(mock_config, "test_table")
            
            # Multiple accesses should reuse the same instance
            result1 = gateway.dynamodb
            result2 = gateway.dynamodb
            
            assert result1 == result2 == mock_dynamodb
            mock_session_class.assert_called_once()

    def test_dynamodb_connection_error(self, mock_config):
        """Test DynamoDB connection error handling."""
        with patch('boto3.Session') as mock_session_class:
            mock_session_class.side_effect = Exception("Connection failed")
            
            gateway = TableGateway(mock_config, "test_table")
            
            with pytest.raises(ConnectionError, match="Failed to connect to DynamoDB"):
                _ = gateway.dynamodb

    def test_table_property_lazy_initialization(self, mock_config):
        """Test lazy initialization of table resource."""
        with patch('boto3.Session') as mock_session_class:
            mock_session = Mock()
            mock_dynamodb = Mock()
            mock_table = Mock()
            mock_session_class.return_value = mock_session
            mock_session.resource.return_value = mock_dynamodb
            mock_dynamodb.Table.return_value = mock_table
            
            gateway = TableGateway(mock_config, "test_table")
            
            # First access should create the table resource
            result = gateway.table
            
            assert result == mock_table
            assert gateway._table == mock_table
            mock_dynamodb.Table.assert_called_once_with("test_table")

    def test_table_access_error(self, mock_config):
        """Test table access error handling."""
        with patch('boto3.Session') as mock_session_class:
            mock_session = Mock()
            mock_dynamodb = Mock()
            mock_session_class.return_value = mock_session
            mock_session.resource.return_value = mock_dynamodb
            mock_dynamodb.Table.side_effect = Exception("Table access failed")
            
            gateway = TableGateway(mock_config, "test_table")
            
            with pytest.raises(ConnectionError, match="Failed to access table"):
                _ = gateway.table

    def test_query_operation(self, mock_config, mock_table):
        """Test query operation."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            query_kwargs = {
                'IndexName': 'TestIndex',
                'KeyConditionExpression': 'test_condition',
                'Limit': 10
            }
            
            result = gateway.query(**query_kwargs)
            
            mock_table.query.assert_called_once_with(**query_kwargs)
            assert result == mock_table.query.return_value

    def test_query_client_error_mapping(self, mock_config, mock_table):
        """Test query operation client error mapping."""
        mock_error = ClientError(
            error_response={'Error': {'Code': 'ValidationException', 'Message': 'Test error'}},
            operation_name='Query'
        )
        mock_table.query.side_effect = mock_error
        
        with patch.object(TableGateway, 'table', mock_table):
            with patch('dynamodb_wrapper.core.table_gateway.map_dynamodb_error') as mock_map:
                mock_map.side_effect = Exception("Mapped error")
                
                gateway = TableGateway(mock_config, "test_table")
                
                with pytest.raises(Exception, match="Mapped error"):
                    gateway.query()
                
                mock_map.assert_called_once_with(mock_error, "Query", "test_table")

    def test_scan_operation(self, mock_config, mock_table):
        """Test scan operation."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            scan_kwargs = {
                'ProjectionExpression': 'id, name',
                'Limit': 100
            }
            
            result = gateway.scan(**scan_kwargs)
            
            mock_table.scan.assert_called_once_with(**scan_kwargs)
            assert result == mock_table.scan.return_value

    def test_scan_without_projection_warning(self, mock_config, mock_table):
        """Test scan operation warns when no projection expression."""
        with patch.object(TableGateway, 'table', mock_table):
            with patch('dynamodb_wrapper.core.table_gateway.logger') as mock_logger:
                gateway = TableGateway(mock_config, "test_table")
                
                gateway.scan()
                
                mock_logger.warning.assert_any_call(
                    "Scan on test_table without ProjectionExpression - consider adding one"
                )

    def test_scan_without_limit_warning(self, mock_config, mock_table):
        """Test scan operation warns when no limit."""
        with patch.object(TableGateway, 'table', mock_table):
            with patch('dynamodb_wrapper.core.table_gateway.logger') as mock_logger:
                gateway = TableGateway(mock_config, "test_table")
                
                gateway.scan(ProjectionExpression='id')
                
                mock_logger.warning.assert_any_call(
                    "Scan on test_table without Limit - consider adding one"
                )

    def test_put_item_operation(self, mock_config, mock_table):
        """Test put_item operation."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            item = {'pipeline_id': 'test', 'name': 'Test Pipeline'}
            condition = Mock()
            
            gateway.put_item(item, condition_expression=condition)
            
            mock_table.put_item.assert_called_once_with(
                Item=item,
                ConditionExpression=condition
            )

    def test_put_item_without_condition(self, mock_config, mock_table):
        """Test put_item operation without condition."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            item = {'pipeline_id': 'test', 'name': 'Test Pipeline'}
            
            gateway.put_item(item)
            
            mock_table.put_item.assert_called_once_with(Item=item)

    def test_put_item_client_error_with_resource_id(self, mock_config, mock_table):
        """Test put_item client error mapping with resource ID."""
        mock_error = ClientError(
            error_response={'Error': {'Code': 'ConditionalCheckFailedException', 'Message': 'Test error'}},
            operation_name='PutItem'
        )
        mock_table.put_item.side_effect = mock_error
        
        with patch.object(TableGateway, 'table', mock_table):
            with patch('dynamodb_wrapper.core.table_gateway.map_dynamodb_error') as mock_map:
                mock_map.side_effect = Exception("Mapped error")
                
                gateway = TableGateway(mock_config, "test_table")
                item = {'pipeline_id': 'test-123', 'name': 'Test'}
                
                with pytest.raises(Exception, match="Mapped error"):
                    gateway.put_item(item)
                
                mock_map.assert_called_once_with(mock_error, "PutItem", "test_table", "test-123")

    def test_update_item_operation(self, mock_config, mock_table):
        """Test update_item operation."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            key = {'pipeline_id': 'test'}
            update_expr = 'SET #name = :name'
            expr_values = {':name': 'Updated Name'}
            expr_names = {'#name': 'name'}
            condition = Mock()
            
            result = gateway.update_item(
                key=key,
                update_expression=update_expr,
                expression_attribute_values=expr_values,
                expression_attribute_names=expr_names,
                condition_expression=condition,
                return_values='ALL_NEW'
            )
            
            mock_table.update_item.assert_called_once_with(
                Key=key,
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_values,
                ExpressionAttributeNames=expr_names,
                ConditionExpression=condition,
                ReturnValues='ALL_NEW'
            )
            assert result == mock_table.update_item.return_value['Attributes']

    def test_update_item_minimal_params(self, mock_config, mock_table):
        """Test update_item with minimal parameters."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            key = {'pipeline_id': 'test'}
            update_expr = 'SET #name = :name'
            
            result = gateway.update_item(key=key, update_expression=update_expr)
            
            mock_table.update_item.assert_called_once_with(
                Key=key,
                UpdateExpression=update_expr,
                ReturnValues='NONE'
            )
            assert result is None

    def test_delete_item_operation(self, mock_config, mock_table):
        """Test delete_item operation."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            key = {'pipeline_id': 'test'}
            condition = Mock()
            
            result = gateway.delete_item(
                key=key,
                condition_expression=condition,
                return_values='ALL_OLD'
            )
            
            mock_table.delete_item.assert_called_once_with(
                Key=key,
                ConditionExpression=condition,
                ReturnValues='ALL_OLD'
            )
            assert result == mock_table.delete_item.return_value['Attributes']

    def test_delete_item_minimal_params(self, mock_config, mock_table):
        """Test delete_item with minimal parameters."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            key = {'pipeline_id': 'test'}
            
            result = gateway.delete_item(key=key)
            
            mock_table.delete_item.assert_called_once_with(
                Key=key,
                ReturnValues='NONE'
            )
            assert result is None

    def test_batch_writer(self, mock_config, mock_table):
        """Test batch_writer operation."""
        mock_batch_writer = Mock()
        mock_table.batch_writer.return_value = mock_batch_writer
        
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            result = gateway.batch_writer()
            
            assert result == mock_batch_writer
            mock_table.batch_writer.assert_called_once()

    def test_transact_write_items(self, mock_config):
        """Test transact_write_items operation."""
        mock_dynamodb = Mock()
        mock_client = Mock()
        mock_dynamodb.meta.client = mock_client
        
        with patch.object(TableGateway, 'dynamodb', mock_dynamodb):
            gateway = TableGateway(mock_config, "test_table")
            
            transact_items = [
                {
                    'Put': {
                        'TableName': 'test_table',
                        'Item': {'pipeline_id': 'test'},
                        'ConditionExpression': 'attribute_not_exists(pipeline_id)'
                    }
                }
            ]
            
            gateway.transact_write_items(transact_items)
            
            mock_client.transact_write_items.assert_called_once_with(
                TransactItems=transact_items
            )

    def test_transact_write_items_client_error(self, mock_config):
        """Test transact_write_items client error mapping."""
        mock_dynamodb = Mock()
        mock_client = Mock()
        mock_error = ClientError(
            error_response={'Error': {'Code': 'TransactionConflictException', 'Message': 'Test error'}},
            operation_name='TransactWriteItems'
        )
        mock_client.transact_write_items.side_effect = mock_error
        mock_dynamodb.meta.client = mock_client
        
        with patch.object(TableGateway, 'dynamodb', mock_dynamodb):
            with patch('dynamodb_wrapper.core.table_gateway.map_dynamodb_error') as mock_map:
                mock_map.side_effect = Exception("Mapped error")
                
                gateway = TableGateway(mock_config, "test_table")
                
                with pytest.raises(Exception, match="Mapped error"):
                    gateway.transact_write_items([])
                
                mock_map.assert_called_once_with(mock_error, "TransactWriteItems", "test_table")

    def test_raw_query_escape_hatch(self, mock_config, mock_table):
        """Test raw_query escape hatch."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            query_kwargs = {'custom_param': 'custom_value'}
            
            result = gateway.raw_query(**query_kwargs)
            
            mock_table.query.assert_called_once_with(**query_kwargs)
            assert result == mock_table.query.return_value

    def test_raw_update_escape_hatch(self, mock_config, mock_table):
        """Test raw_update escape hatch."""
        with patch.object(TableGateway, 'table', mock_table):
            gateway = TableGateway(mock_config, "test_table")
            
            update_kwargs = {'custom_param': 'custom_value'}
            
            result = gateway.raw_update(**update_kwargs)
            
            mock_table.update_item.assert_called_once_with(**update_kwargs)
            assert result == mock_table.update_item.return_value

    def test_raw_update_client_error(self, mock_config, mock_table):
        """Test raw_update client error mapping."""
        mock_error = ClientError(
            error_response={'Error': {'Code': 'ValidationException', 'Message': 'Test error'}},
            operation_name='UpdateItem'
        )
        mock_table.update_item.side_effect = mock_error
        
        with patch.object(TableGateway, 'table', mock_table):
            with patch('dynamodb_wrapper.core.table_gateway.map_dynamodb_error') as mock_map:
                mock_map.side_effect = Exception("Mapped error")
                
                gateway = TableGateway(mock_config, "test_table")
                
                with pytest.raises(Exception, match="Mapped error"):
                    gateway.raw_update()
                
                mock_map.assert_called_once_with(mock_error, "RawUpdateItem", "test_table")


class TestCreateTableGateway:
    """Test create_table_gateway factory function."""

    def test_create_table_gateway(self, mock_config):
        """Test create_table_gateway factory function."""
        result = create_table_gateway(mock_config, "table_name")
        
        assert isinstance(result, TableGateway)
        assert result.config == mock_config
        # The table name will be generated based on the config's get_table_name method
        expected_name = mock_config.get_table_name("table_name")
        assert result.table_name == expected_name

    def test_create_table_gateway_uses_config_prefix(self, mock_config):
        """Test that create_table_gateway uses config table name resolution."""
        gateway = create_table_gateway(mock_config, "pipeline_config")
        
        # Verify that the table name was resolved using the config's method
        expected_table_name = mock_config.get_table_name("pipeline_config") 
        assert gateway.table_name == expected_table_name
        
        # For a config with environment "dev", the expected pattern is: dev_pipeline_config
        assert "pipeline_config" in gateway.table_name


class TestTableGatewayConfiguration:
    """Test TableGateway configuration handling."""

    def test_boto3_config_with_endpoint_url(self, mock_config):
        """Test boto3 configuration with custom endpoint URL."""
        mock_config.endpoint_url = "http://localhost:8000"
        
        with patch('boto3.Session') as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session
            
            gateway = TableGateway(mock_config, "test_table")
            _ = gateway.dynamodb
            
            # Verify endpoint_url is passed to resource creation
            resource_kwargs = mock_session.resource.call_args[1]
            assert resource_kwargs['endpoint_url'] == "http://localhost:8000"

    def test_boto3_config_without_endpoint_url(self, mock_config):
        """Test boto3 configuration without endpoint URL."""
        mock_config.endpoint_url = None
        
        with patch('boto3.Session') as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value = mock_session
            
            gateway = TableGateway(mock_config, "test_table")
            _ = gateway.dynamodb
            
            # Verify endpoint_url is not passed to resource creation
            resource_kwargs = mock_session.resource.call_args[1]
            assert 'endpoint_url' not in resource_kwargs

    def test_boto3_retry_configuration(self, mock_config):
        """Test boto3 retry and timeout configuration."""
        mock_config.retries = 5
        mock_config.max_pool_connections = 20
        mock_config.timeout_seconds = 30
        
        with patch('boto3.Session') as mock_session_class:
            with patch('dynamodb_wrapper.core.table_gateway.Config') as mock_config_class:
                mock_session = Mock()
                mock_boto_config = Mock()
                mock_session_class.return_value = mock_session
                mock_config_class.return_value = mock_boto_config
                
                gateway = TableGateway(mock_config, "test_table")
                _ = gateway.dynamodb
                
                # Verify Config is created with correct parameters
                mock_config_class.assert_called_once_with(
                    retries={'max_attempts': 5},
                    max_pool_connections=20,
                    read_timeout=30,
                    connect_timeout=30
                )
                
                # Verify config is passed to resource creation
                resource_kwargs = mock_session.resource.call_args[1]
                assert resource_kwargs['config'] == mock_boto_config