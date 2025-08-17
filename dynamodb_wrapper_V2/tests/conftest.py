"""
Test configuration and fixtures for DynamoDB Wrapper V2 CQRS architecture.

Provides common fixtures for testing the V2 CQRS APIs with mocked DynamoDB resources.
"""

import sys
import time
import subprocess
import requests
from pathlib import Path
from typing import Generator

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


# ===== LocalStack Integration Test Fixtures =====

@pytest.fixture(scope="session")
def localstack_container() -> Generator[None, None, None]:
    """Start LocalStack container for integration tests."""
    try:
        # Check if LocalStack is already running
        response = requests.get("http://localhost:4566/_localstack/health", timeout=2)
        if response.status_code == 200:
            print("LocalStack is already running")
            yield
            return
    except (requests.RequestException, requests.ConnectionError):
        pass  # LocalStack not running, start it
    
    # Start LocalStack container
    compose_file = Path(__file__).parent.parent / "docker-compose.localstack.yml"
    
    print("Starting LocalStack container...")
    try:
        subprocess.run([
            "docker-compose", 
            "-f", str(compose_file), 
            "up", "-d"
        ], check=True, capture_output=True)
        
        # Wait for LocalStack to be ready
        _wait_for_localstack()
        
        yield
        
    finally:
        # Clean up: stop LocalStack container
        print("Stopping LocalStack container...")
        try:
            subprocess.run([
                "docker-compose", 
                "-f", str(compose_file), 
                "down"
            ], check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            print(f"Warning: Failed to stop LocalStack container: {e}")


def _wait_for_localstack(max_retries: int = 30, delay: float = 1.0) -> None:
    """Wait for LocalStack to be ready."""
    print("Waiting for LocalStack to be ready...")
    
    for attempt in range(max_retries):
        try:
            response = requests.get("http://localhost:4566/_localstack/health", timeout=2)
            if response.status_code == 200:
                health_data = response.json()
                if health_data.get("services", {}).get("dynamodb") == "available":
                    print("LocalStack DynamoDB is ready!")
                    return
        except (requests.RequestException, requests.ConnectionError):
            pass
        
        print(f"Attempt {attempt + 1}/{max_retries}: LocalStack not ready yet...")
        time.sleep(delay)
    
    raise RuntimeError("LocalStack failed to start within the expected time")


@pytest.fixture
def localstack_config(localstack_container):
    """DynamoDB configuration for LocalStack integration testing."""
    return DynamoDBConfig(
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        endpoint_url="http://localhost:4566",
        environment="dev",
        table_prefix="integration"
    )


@pytest.fixture
def localstack_dynamodb_resource(localstack_container):
    """LocalStack DynamoDB resource for integration testing."""
    return boto3.resource(
        'dynamodb',
        region_name='us-east-1',
        endpoint_url='http://localhost:4566',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )


@pytest.fixture
def localstack_tables(localstack_dynamodb_resource):
    """Create all DynamoDB tables in LocalStack for integration testing."""
    tables_config = [
        {
            'name': 'integration_dev_pipeline_config',
            'key_schema': [{'AttributeName': 'pipeline_id', 'KeyType': 'HASH'}],
            'attributes': [
                {'AttributeName': 'pipeline_id', 'AttributeType': 'S'},
                {'AttributeName': 'is_active', 'AttributeType': 'S'},
                {'AttributeName': 'environment', 'AttributeType': 'S'},
                {'AttributeName': 'updated_at', 'AttributeType': 'S'},
                {'AttributeName': 'created_at', 'AttributeType': 'S'}
            ],
            'gsi': [
                {
                    'IndexName': 'ActivePipelinesIndex',
                    'KeySchema': [
                        {'AttributeName': 'is_active', 'KeyType': 'HASH'},
                        {'AttributeName': 'updated_at', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                },
                {
                    'IndexName': 'EnvironmentIndex',
                    'KeySchema': [
                        {'AttributeName': 'environment', 'KeyType': 'HASH'},
                        {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]
        },
        {
            'name': 'integration_dev_table_config',
            'key_schema': [{'AttributeName': 'table_id', 'KeyType': 'HASH'}],
            'attributes': [
                {'AttributeName': 'table_id', 'AttributeType': 'S'},
                {'AttributeName': 'pipeline_id', 'AttributeType': 'S'},
                {'AttributeName': 'table_type', 'AttributeType': 'S'},
                {'AttributeName': 'created_at', 'AttributeType': 'S'}
            ],
            'gsi': [
                {
                    'IndexName': 'PipelineTablesIndex',
                    'KeySchema': [
                        {'AttributeName': 'pipeline_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                },
                {
                    'IndexName': 'TypeIndex',
                    'KeySchema': [
                        {'AttributeName': 'table_type', 'KeyType': 'HASH'},
                        {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]
        },
        {
            'name': 'integration_dev_pipeline_run_logs',
            'key_schema': [
                {'AttributeName': 'run_id', 'KeyType': 'HASH'},
                {'AttributeName': 'pipeline_id', 'KeyType': 'RANGE'}
            ],
            'attributes': [
                {'AttributeName': 'run_id', 'AttributeType': 'S'},
                {'AttributeName': 'pipeline_id', 'AttributeType': 'S'},
                {'AttributeName': 'status', 'AttributeType': 'S'},
                {'AttributeName': 'created_at', 'AttributeType': 'S'}
            ],
            'gsi': [
                {
                    'IndexName': 'PipelineRunsIndex',
                    'KeySchema': [
                        {'AttributeName': 'pipeline_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                },
                {
                    'IndexName': 'StatusIndex',
                    'KeySchema': [
                        {'AttributeName': 'status', 'KeyType': 'HASH'},
                        {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ]
        }
    ]
    
    created_tables = {}
    
    for table_config in tables_config:
        # Check if table already exists
        try:
            table = localstack_dynamodb_resource.Table(table_config['name'])
            table.load()
            print(f"Table {table_config['name']} already exists")
            created_tables[table_config['name']] = table
            continue
        except localstack_dynamodb_resource.meta.client.exceptions.ResourceNotFoundException:
            pass  # Table doesn't exist, create it
        
        # Create table with GSIs
        table = localstack_dynamodb_resource.create_table(
            TableName=table_config['name'],
            KeySchema=table_config['key_schema'],
            AttributeDefinitions=table_config['attributes'],
            GlobalSecondaryIndexes=[
                {
                    **gsi,
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
                for gsi in table_config.get('gsi', [])
            ],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        
        # Wait for table to be active
        table.wait_until_exists()
        print(f"Created table: {table_config['name']}")
        created_tables[table_config['name']] = table
    
    return created_tables


@pytest.fixture
def localstack_pipeline_config_read_api(localstack_config, localstack_tables):
    """Pipeline configuration read API with LocalStack DynamoDB."""
    return PipelineConfigReadApi(localstack_config)


@pytest.fixture
def localstack_pipeline_config_write_api(localstack_config, localstack_tables):
    """Pipeline configuration write API with LocalStack DynamoDB."""
    return PipelineConfigWriteApi(localstack_config)


@pytest.fixture
def localstack_table_config_read_api(localstack_config, localstack_tables):
    """Table configuration read API with LocalStack DynamoDB."""
    return TableConfigReadApi(localstack_config)


@pytest.fixture
def localstack_table_config_write_api(localstack_config, localstack_tables):
    """Table configuration write API with LocalStack DynamoDB."""
    return TableConfigWriteApi(localstack_config)


@pytest.fixture
def localstack_pipeline_run_logs_read_api(localstack_config, localstack_tables):
    """Pipeline run logs read API with LocalStack DynamoDB."""
    return PipelineRunLogsReadApi(localstack_config)


@pytest.fixture
def localstack_pipeline_run_logs_write_api(localstack_config, localstack_tables):
    """Pipeline run logs write API with LocalStack DynamoDB."""
    return PipelineRunLogsWriteApi(localstack_config)