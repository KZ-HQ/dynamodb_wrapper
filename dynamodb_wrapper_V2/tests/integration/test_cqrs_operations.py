"""
End-to-End CQRS Operations Tests

These tests verify the Command Query Responsibility Segregation pattern
implementation with real DynamoDB operations.

Key testing scenarios:
1. Read/Write API separation and optimization
2. Query performance with projections and pagination
3. Write operations with conditional expressions and transactions
4. Cross-domain operations and data consistency
"""

import pytest
from datetime import datetime, timezone, timedelta
from moto import mock_aws
import boto3
from typing import List, Optional

from dynamodb_wrapper import (
    DynamoDBConfig,
    PipelineConfigReadApi,
    PipelineConfigWriteApi,
    TableConfigReadApi,
    TableConfigWriteApi,
    PipelineRunLogsReadApi,
    PipelineRunLogsWriteApi,
    PipelineConfigUpsert,
    TableConfigUpsert,
    PipelineRunLogUpsert
)
# Removed timezone-specific imports after TimezoneManager simplification


@pytest.fixture
def cqrs_config():
    """Configuration for CQRS testing."""
    return DynamoDBConfig(
        aws_access_key_id="test_key",
        aws_secret_access_key="test_secret",
        region_name="us-east-1",
        endpoint_url=None,
        environment="dev",
        table_prefix="cqrs_test"
    )


@pytest.fixture
def mock_dynamodb_full():
    """Create mock DynamoDB with all tables for comprehensive testing."""
    with mock_aws():
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        # Create all required tables
        tables_config = [
            {
                'name': 'cqrs_test_dev_pipeline_config',
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
                ]
            },
            {
                'name': 'cqrs_test_dev_table_config',
                'key_schema': [{'AttributeName': 'table_id', 'KeyType': 'HASH'}],
                'attributes': [
                    {'AttributeName': 'table_id', 'AttributeType': 'S'},
                    {'AttributeName': 'pipeline_id', 'AttributeType': 'S'},
                    {'AttributeName': 'created_at', 'AttributeType': 'S'}
                ],
                'gsi': [
                    {
                        'IndexName': 'PipelineIndex',
                        'KeySchema': [
                            {'AttributeName': 'pipeline_id', 'KeyType': 'HASH'},
                            {'AttributeName': 'created_at', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                    }
                ]
            },
            {
                'name': 'cqrs_test_dev_pipeline_run_logs',
                'key_schema': [{'AttributeName': 'run_id', 'KeyType': 'HASH'}],
                'attributes': [
                    {'AttributeName': 'run_id', 'AttributeType': 'S'},
                    {'AttributeName': 'pipeline_id', 'AttributeType': 'S'},
                    {'AttributeName': 'status', 'AttributeType': 'S'},
                    {'AttributeName': 'created_at', 'AttributeType': 'S'}
                ],
                'gsi': [
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
                ]
            }
        ]
        
        # Create each table
        for table_config in tables_config:
            table = dynamodb.create_table(
                TableName=table_config['name'],
                KeySchema=table_config['key_schema'],
                AttributeDefinitions=table_config['attributes'],
                GlobalSecondaryIndexes=table_config.get('gsi', []),
                BillingMode='PROVISIONED',
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
        
        yield dynamodb


class TestCQRSOperations:
    """Test CQRS pattern implementation with real operations."""

    def test_read_api_query_optimization(self, cqrs_config, mock_dynamodb_full):
        """Test read API query optimization with projections and pagination."""
        write_api = PipelineConfigWriteApi(cqrs_config)
        read_api = PipelineConfigReadApi(cqrs_config)
        
        # Create multiple pipelines for testing queries
        pipelines_data = []
        for i in range(10):
            pipeline_data = PipelineConfigUpsert(
                pipeline_id=f"query-test-pipeline-{i:02d}",
                pipeline_name=f"Query Test Pipeline {i}",
                description=f"Pipeline {i} for testing query optimization",
                source_type="s3",
                destination_type="warehouse",
                environment="dev",
                is_active=i % 2 == 0,  # Alternate active/inactive
                created_by=f"user-{i}"
            )
            pipelines_data.append(pipeline_data)
        
        # Batch create pipelines
        created_pipelines = write_api.upsert_many(pipelines_data)
        assert len(created_pipelines) == 10
        
        # Test 1: Query with minimal projection (optimized read)
        active_pipelines, next_key = read_api.query_active_pipelines(
            projection=['pipeline_id', 'pipeline_name', 'is_active'],
            limit=3
        )
        
        assert len(active_pipelines) == 3
        assert next_key is not None  # Should have more pages
        assert all(pipeline.is_active for pipeline in active_pipelines)
        
        # Test 2: Pagination continuation
        more_pipelines, final_key = read_api.query_active_pipelines(
            projection=['pipeline_id', 'pipeline_name'],
            limit=5,
            last_key=next_key
        )
        
        assert len(more_pipelines) <= 5
        # Should not return any pipeline already returned in first page
        first_ids = {p.pipeline_id for p in active_pipelines}
        second_ids = {p.pipeline_id for p in more_pipelines}
        assert first_ids.isdisjoint(second_ids)
        
        # Test 3: Environment-based query
        dev_pipelines, _ = read_api.query_by_environment("dev", limit=20)
        assert len(dev_pipelines) == 10  # All pipelines are in dev
        
        # Test 4: Combined environment and status query  
        active_dev_pipelines, _ = read_api.query_by_environment_and_status(
            "dev", True, limit=10
        )
        assert len(active_dev_pipelines) == 5  # Half are active
        assert all(p.is_active for p in active_dev_pipelines)

    def test_write_api_conditional_operations(self, cqrs_config, mock_dynamodb_full):
        """Test write API conditional operations and safety checks."""
        write_api = PipelineConfigWriteApi(cqrs_config)
        read_api = PipelineConfigReadApi(cqrs_config)
        
        # Test 1: Create with existence check (should succeed)
        pipeline_data = PipelineConfigUpsert(
            pipeline_id="conditional-test-pipeline",
            pipeline_name="Conditional Test Pipeline",
            description="Testing conditional operations",
            source_type="s3",
            destination_type="warehouse",
            environment="dev",
            is_active=True,
            created_by="conditional-user"
        )
        
        created_pipeline = write_api.create_pipeline(pipeline_data)
        assert created_pipeline.pipeline_id == "conditional-test-pipeline"
        
        # Test 2: Attempt to create duplicate (should fail due to condition)
        with pytest.raises(Exception):  # Should be ConflictError
            write_api.create_pipeline(pipeline_data)
        
        # Test 3: Update existing pipeline (should succeed)
        updated_attrs = write_api.update_pipeline(
            "conditional-test-pipeline",
            {
                "description": "Updated description",
                "updated_by": "update-user"
            }
        )
        assert updated_attrs is not None
        
        # Test 4: Update non-existent pipeline (should fail)
        with pytest.raises(Exception):  # Should be ItemNotFoundError or ConflictError
            write_api.update_pipeline(
                "non-existent-pipeline",
                {"description": "This should fail"}
            )
        
        # Test 5: Delete with existence check
        success = write_api.delete_pipeline("conditional-test-pipeline")
        assert success is True
        
        # Verify deletion
        deleted_pipeline = read_api.get_by_id("conditional-test-pipeline")
        assert deleted_pipeline is None

    # Removed test_transaction_operations_timezone_consistency - no longer needed after TimezoneManager simplification

    # Removed test_cross_domain_operations_timezone_handling - no longer needed after TimezoneManager simplification
    def test_read_write_performance_patterns(self, cqrs_config, mock_dynamodb_full):
        """Test CQRS read/write performance optimization patterns."""
        write_api = PipelineConfigWriteApi(cqrs_config)
        read_api = PipelineConfigReadApi(cqrs_config)
        
        # Create many pipelines to test performance patterns
        pipelines_data = []
        for i in range(20):
            pipeline_data = PipelineConfigUpsert(
                pipeline_id=f"perf-pipeline-{i:03d}",
                pipeline_name=f"Performance Pipeline {i}",
                description=f"Pipeline {i} for performance testing",
                source_type="s3",
                destination_type="warehouse",
                environment="dev" if i < 10 else "staging",
                is_active=i % 3 == 0,  # Every third is active
                created_by=f"perf-user-{i % 3}"
            )
            pipelines_data.append(pipeline_data)
        
        # Batch write optimization
        created_pipelines = write_api.upsert_many(pipelines_data)
        assert len(created_pipelines) == 20
        
        # Read optimization 1: Summary view (minimal data)
        summary = read_api.get_pipeline_summary("perf-pipeline-001")
        assert summary is not None
        # Summary should have minimal fields
        assert hasattr(summary, 'pipeline_id')
        assert hasattr(summary, 'pipeline_name')
        
        # Read optimization 2: Count operations (no data transfer)
        dev_count = read_api.count_pipelines_by_environment("dev")
        staging_count = read_api.count_pipelines_by_environment("staging")
        active_count = read_api.count_active_pipelines()
        
        assert dev_count == 10
        assert staging_count == 10
        assert active_count == 7  # Every third of 20 (0, 3, 6, 9, 12, 15, 18)
        
        # Read optimization 3: Projection queries (minimal data transfer)
        projected_pipelines, _ = read_api.query_by_environment(
            "dev",
            projection=['pipeline_id', 'pipeline_name', 'is_active'],
            limit=5
        )
        
        assert len(projected_pipelines) == 5
        # Verify projected fields are available
        for pipeline in projected_pipelines:
            assert pipeline.pipeline_id is not None
            assert pipeline.pipeline_name is not None
            assert pipeline.is_active is not None

    def test_data_consistency_across_operations(self, cqrs_config, mock_dynamodb_full):
        """Test data consistency across different operation types."""
        write_api = PipelineConfigWriteApi(cqrs_config)
        read_api = PipelineConfigReadApi(cqrs_config)
        # TimezoneManager removed - using Python's built-in datetime
        
        # Create initial pipeline
        pipeline_data = PipelineConfigUpsert(
            pipeline_id="consistency-test-pipeline",
            pipeline_name="Consistency Test",
            description="Testing data consistency",
            source_type="s3",
            destination_type="warehouse",
            environment="dev",
            is_active=False,
            created_by="consistency-user"
        )
        
        created = write_api.create_pipeline(pipeline_data)
        original_created_at = created.created_at
        original_updated_at = created.updated_at
        
        # Perform update
        write_api.update_pipeline_status("consistency-test-pipeline", True, "admin-user")
        
        # Read back and verify consistency
        updated = read_api.get_by_id("consistency-test-pipeline")
        assert updated.is_active is True
        assert updated.created_at == original_created_at  # Should not change
        assert updated.updated_at > original_updated_at   # Should be newer
        
        # Perform upsert (should preserve created_at)
        upsert_data = PipelineConfigUpsert(
            pipeline_id="consistency-test-pipeline",
            pipeline_name="Updated via Upsert",
            description="Updated description",
            source_type="kafka",  # Changed
            destination_type="warehouse",
            environment="dev", 
            is_active=True,
            created_by="upsert-user"
        )
        
        upserted = write_api.upsert_pipeline(upsert_data)
        
        # Verify upsert maintained creation timestamp but updated other fields
        assert upserted.pipeline_name == "Updated via Upsert"
        assert upserted.source_type == "kafka"
        assert upserted.created_at == original_created_at  # Preserved
        assert upserted.updated_at > updated.updated_at   # Newer than last update

    # Removed test_timezone_conversion_edge_cases_e2e - no longer needed after TimezoneManager simplification
    # Removed test_bulk_operations_timezone_consistency - no longer needed after TimezoneManager simplification
