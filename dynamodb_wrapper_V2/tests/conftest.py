"""
Test configuration and fixtures for DynamoDB Wrapper V2 CQRS architecture.

Provides common fixtures for testing the V2 CQRS APIs with mocked DynamoDB resources.
"""

import sys
from pathlib import Path

# Add parent directory to path so we can import dynamodb_wrapper
sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
import pytest
from moto import mock_aws

from dynamodb_wrapper import (
    DynamoDBConfig,
    PipelineConfigReadApi,
    PipelineConfigWriteApi,
    TableConfigReadApi,
    TableConfigWriteApi,
    PipelineRunLogsReadApi,
    PipelineRunLogsWriteApi,
)


@pytest.fixture
def dynamodb_config():
    """DynamoDB configuration for testing."""
    return DynamoDBConfig(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        region_name="us-east-1",
        endpoint_url="http://localhost:8000",
        environment="test",
        table_prefix="test_"
    )


@pytest.fixture
def mock_dynamodb_config():
    """DynamoDB configuration for mocked testing."""
    return DynamoDBConfig(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        region_name="us-east-1",
        endpoint_url=None,  # Use default AWS endpoint for moto
        environment="test",
        table_prefix="test_"
    )


@pytest.fixture
def mock_dynamodb_resource():
    """Mock DynamoDB resource."""
    with mock_aws():
        yield boto3.resource('dynamodb', region_name='us-east-1')


@pytest.fixture
def pipeline_config_table(mock_dynamodb_resource):
    """Create pipeline_config table for testing."""
    table = mock_dynamodb_resource.create_table(
        TableName='test_pipeline_config',
        KeySchema=[
            {'AttributeName': 'pipeline_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'pipeline_id', 'AttributeType': 'S'},
            {'AttributeName': 'is_active', 'AttributeType': 'S'},
            {'AttributeName': 'environment', 'AttributeType': 'S'},
            {'AttributeName': 'updated_at', 'AttributeType': 'S'},
            {'AttributeName': 'created_at', 'AttributeType': 'S'}
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'ActivePipelinesIndex',
                'KeySchema': [
                    {'AttributeName': 'is_active', 'KeyType': 'HASH'},
                    {'AttributeName': 'updated_at', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            {
                'IndexName': 'EnvironmentIndex',
                'KeySchema': [
                    {'AttributeName': 'environment', 'KeyType': 'HASH'},
                    {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }
        ],
        BillingMode='PROVISIONED',
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    return table


@pytest.fixture
def table_config_table(mock_dynamodb_resource):
    """Create table_config table for testing."""
    table = mock_dynamodb_resource.create_table(
        TableName='test_table_config',
        KeySchema=[
            {'AttributeName': 'table_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'table_id', 'AttributeType': 'S'},
            {'AttributeName': 'pipeline_id', 'AttributeType': 'S'},
            {'AttributeName': 'table_type', 'AttributeType': 'S'},
            {'AttributeName': 'created_at', 'AttributeType': 'S'}
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'PipelineIndex',
                'KeySchema': [
                    {'AttributeName': 'pipeline_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            {
                'IndexName': 'TypeIndex',
                'KeySchema': [
                    {'AttributeName': 'table_type', 'KeyType': 'HASH'},
                    {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }
        ],
        BillingMode='PROVISIONED',
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    return table


@pytest.fixture
def pipeline_run_logs_table(mock_dynamodb_resource):
    """Create pipeline_run_logs table for testing."""
    table = mock_dynamodb_resource.create_table(
        TableName='test_pipeline_run_logs',
        KeySchema=[
            {'AttributeName': 'run_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'run_id', 'AttributeType': 'S'},
            {'AttributeName': 'pipeline_id', 'AttributeType': 'S'},
            {'AttributeName': 'status', 'AttributeType': 'S'},
            {'AttributeName': 'created_at', 'AttributeType': 'S'}
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'PipelineIndex',
                'KeySchema': [
                    {'AttributeName': 'pipeline_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            },
            {
                'IndexName': 'StatusIndex',
                'KeySchema': [
                    {'AttributeName': 'status', 'KeyType': 'HASH'},
                    {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }
        ],
        BillingMode='PROVISIONED',
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    return table


@pytest.fixture
def all_tables(pipeline_config_table, table_config_table, pipeline_run_logs_table):
    """Create all DynamoDB tables needed for comprehensive testing."""
    return {
        'pipeline_config': pipeline_config_table,
        'table_config': table_config_table,
        'pipeline_run_logs': pipeline_run_logs_table
    }


# CQRS API Fixtures

@pytest.fixture
def pipeline_config_read_api(mock_dynamodb_config, pipeline_config_table):
    """Pipeline configuration read API with mocked DynamoDB."""
    return PipelineConfigReadApi(mock_dynamodb_config)


@pytest.fixture
def pipeline_config_write_api(mock_dynamodb_config, pipeline_config_table):
    """Pipeline configuration write API with mocked DynamoDB."""
    return PipelineConfigWriteApi(mock_dynamodb_config)


@pytest.fixture
def table_config_read_api(mock_dynamodb_config, table_config_table):
    """Table configuration read API with mocked DynamoDB."""
    return TableConfigReadApi(mock_dynamodb_config)


@pytest.fixture
def table_config_write_api(mock_dynamodb_config, table_config_table):
    """Table configuration write API with mocked DynamoDB."""
    return TableConfigWriteApi(mock_dynamodb_config)


@pytest.fixture
def pipeline_run_logs_read_api(mock_dynamodb_config, pipeline_run_logs_table):
    """Pipeline run logs read API with mocked DynamoDB."""
    return PipelineRunLogsReadApi(mock_dynamodb_config)


@pytest.fixture
def pipeline_run_logs_write_api(mock_dynamodb_config, pipeline_run_logs_table):
    """Pipeline run logs write API with mocked DynamoDB."""
    return PipelineRunLogsWriteApi(mock_dynamodb_config)


# Sample Data Fixtures

@pytest.fixture
def sample_pipeline_data():
    """Sample pipeline configuration data for testing."""
    return {
        "pipeline_id": "test-pipeline-1",
        "pipeline_name": "Test Data Pipeline",
        "description": "A test pipeline for unit testing",
        "source_type": "s3",
        "destination_type": "warehouse",
        "is_active": True,
        "environment": "test",
        "created_by": "test-user"
    }


@pytest.fixture
def sample_table_data():
    """Sample table configuration data for testing."""
    return {
        "table_id": "test-table-1", 
        "pipeline_id": "test-pipeline-1",
        "table_name": "test_table",
        "table_type": "source",
        "data_format": "parquet",
        "location": "s3://test-bucket/data/",
        "is_active": True,
        "environment": "test",
        "created_by": "test-user"
    }


@pytest.fixture  
def sample_run_log_data():
    """Sample pipeline run log data for testing."""
    return {
        "run_id": "test-run-1",
        "pipeline_id": "test-pipeline-1", 
        "status": "running",
        "trigger_type": "manual",
        "environment": "test",
        "created_by": "test-user"
    }