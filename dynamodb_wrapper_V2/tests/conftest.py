import sys
from pathlib import Path

# Add parent directory to path so we can import dynamodb_wrapper_V2
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import boto3
import pytest
from moto import mock_aws

from dynamodb_wrapper_V2.dynamodb_wrapper.config import DynamoDBConfig
from dynamodb_wrapper_V2.dynamodb_wrapper.repositories import (
    PipelineConfigRepository,
    PipelineRunLogsRepository,
    TableConfigRepository,
)


@pytest.fixture
def dynamodb_config():
    """DynamoDB configuration for testing."""
    return DynamoDBConfig(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        region_name="us-east-1",
        endpoint_url="http://localhost:8000",
        environment="test"
    )


@pytest.fixture
def mock_dynamodb_config():
    """DynamoDB configuration for mocked testing."""
    return DynamoDBConfig(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        region_name="us-east-1",
        endpoint_url=None,  # Use default AWS endpoint for moto
        environment="dev"
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
        TableName='pipeline_config',
        KeySchema=[
            {'AttributeName': 'pipeline_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'pipeline_id', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    return table


@pytest.fixture
def table_config_table(mock_dynamodb_resource):
    """Create table_config table for testing."""
    table = mock_dynamodb_resource.create_table(
        TableName='table_config',
        KeySchema=[
            {'AttributeName': 'table_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'table_id', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    return table


@pytest.fixture
def pipeline_run_logs_table(mock_dynamodb_resource):
    """Create pipeline_run_logs table for testing."""
    table = mock_dynamodb_resource.create_table(
        TableName='pipeline_run_logs',
        KeySchema=[
            {'AttributeName': 'run_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'run_id', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    return table


@pytest.fixture
def test_table(mock_dynamodb_resource):
    """Create a generic test table for base repository testing."""
    table = mock_dynamodb_resource.create_table(
        TableName='dev_test_table',
        KeySchema=[
            {'AttributeName': 'pipeline_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'pipeline_id', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    return table


@pytest.fixture
def all_tables(pipeline_config_table, table_config_table, pipeline_run_logs_table, test_table):
    """Create all DynamoDB tables needed for comprehensive testing."""
    return {
        'pipeline_config': pipeline_config_table,
        'table_config': table_config_table,
        'pipeline_run_logs': pipeline_run_logs_table,
        'test_table': test_table
    }


@pytest.fixture
def pipeline_repo(dynamodb_config):
    """Pipeline configuration repository."""
    return PipelineConfigRepository(dynamodb_config)


@pytest.fixture
def table_repo(dynamodb_config):
    """Table configuration repository."""
    return TableConfigRepository(dynamodb_config)


@pytest.fixture
def logs_repo(dynamodb_config):
    """Pipeline run logs repository."""
    return PipelineRunLogsRepository(dynamodb_config)


@pytest.fixture
def mock_pipeline_repo(mock_dynamodb_config):
    """Mock pipeline configuration repository."""
    return PipelineConfigRepository(mock_dynamodb_config)


@pytest.fixture
def mock_table_repo(mock_dynamodb_config):
    """Mock table configuration repository."""
    return TableConfigRepository(mock_dynamodb_config)


@pytest.fixture
def mock_logs_repo(mock_dynamodb_config):
    """Mock pipeline run logs repository."""
    return PipelineRunLogsRepository(mock_dynamodb_config)
