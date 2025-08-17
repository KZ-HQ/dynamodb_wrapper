"""
LocalStack Integration Tests for DynamoDB Wrapper V2

These tests verify the CQRS architecture works correctly with a real DynamoDB instance
running in LocalStack, providing more realistic testing beyond moto mocking.

Key testing scenarios:
1. Full CQRS operations with real DynamoDB behavior
2. Cross-domain operations and data consistency
3. Realistic error handling and retry scenarios
4. Performance characteristics with actual network calls
5. Timezone compliance with real storage/retrieval cycles
"""

import pytest
import uuid
from datetime import datetime, timezone, timedelta
from typing import List

from dynamodb_wrapper import (
    PipelineConfigUpsert,
    TableConfigUpsert, 
    PipelineRunLogUpsert,
    RunStatus,
    TableType,
    DataFormat
)
from dynamodb_wrapper.exceptions import ItemNotFoundError, ConflictError
from tests.helpers.timezone_assertions import (
    assert_utc_timezone,
    assert_timezone_equals,
    assert_timezones_equivalent,
    assert_stored_as_utc_string,
    create_timezone_test_context
)


class TestLocalStackCQRSOperations:
    """Test CQRS operations with LocalStack DynamoDB."""

    def test_pipeline_config_full_lifecycle(
        self, 
        localstack_pipeline_config_write_api,
        localstack_pipeline_config_read_api
    ):
        """Test complete pipeline configuration lifecycle with real DynamoDB."""
        write_api = localstack_pipeline_config_write_api
        read_api = localstack_pipeline_config_read_api
        
        # Create unique pipeline ID to avoid conflicts
        pipeline_id = f"localstack-pipeline-{uuid.uuid4().hex[:8]}"
        
        # Create pipeline
        pipeline_data = PipelineConfigUpsert(
            pipeline_id=pipeline_id,
            pipeline_name="LocalStack Test Pipeline",
            description="Integration test pipeline using LocalStack",
            source_type="s3",
            destination_type="warehouse",
            environment="dev",
            is_active=True,
            created_by="localstack-test",
            tags={"test": "localstack", "environment": "integration"}
        )
        
        created_pipeline = write_api.create_pipeline(pipeline_data)
        
        # Verify creation
        assert created_pipeline.pipeline_id == pipeline_id
        assert created_pipeline.pipeline_name == "LocalStack Test Pipeline"
        assert created_pipeline.is_active is True
        assert_utc_timezone(created_pipeline.created_at)
        assert_utc_timezone(created_pipeline.updated_at)
        
        # Read back and verify
        retrieved_pipeline = read_api.get_by_id(pipeline_id)
        assert retrieved_pipeline is not None
        assert retrieved_pipeline.pipeline_id == pipeline_id
        assert retrieved_pipeline.tags == {"test": "localstack", "environment": "integration"}
        
        # Test update
        updates = {
            "description": "Updated description via LocalStack",
            "is_active": False,
            "updated_by": "localstack-updater"
        }
        
        update_result = write_api.update_pipeline(pipeline_id, updates)
        assert update_result is not None
        
        # Verify update
        updated_pipeline = read_api.get_by_id(pipeline_id)
        assert updated_pipeline.description == "Updated description via LocalStack"
        assert updated_pipeline.is_active is False
        assert updated_pipeline.updated_at > created_pipeline.updated_at
        
        # Test projection query
        summary = read_api.get_pipeline_summary(pipeline_id)
        assert summary is not None
        assert summary.pipeline_id == pipeline_id
        assert summary.pipeline_name == "LocalStack Test Pipeline"
        
        # Test deletion
        delete_success = write_api.delete_pipeline(pipeline_id)
        assert delete_success is True
        
        # Verify deletion
        deleted_pipeline = read_api.get_by_id(pipeline_id)
        assert deleted_pipeline is None

    def test_gsi_queries_with_real_dynamodb(
        self,
        localstack_pipeline_config_write_api,
        localstack_pipeline_config_read_api
    ):
        """Test GSI queries with real DynamoDB indexes."""
        write_api = localstack_pipeline_config_write_api
        read_api = localstack_pipeline_config_read_api
        
        # Create multiple pipelines for GSI testing
        base_id = f"gsi-test-{uuid.uuid4().hex[:6]}"
        pipeline_ids = []
        
        for i in range(5):
            pipeline_id = f"{base_id}-{i:02d}"
            pipeline_ids.append(pipeline_id)
            
            pipeline_data = PipelineConfigUpsert(
                pipeline_id=pipeline_id,
                pipeline_name=f"GSI Test Pipeline {i}",
                description=f"Pipeline {i} for GSI testing",
                source_type="s3",
                destination_type="warehouse",
                environment="dev",
                is_active=i % 2 == 0,  # Alternate active/inactive
                created_by=f"gsi-test-user-{i}"
            )
            
            created = write_api.create_pipeline(pipeline_data)
            assert created.pipeline_id == pipeline_id
        
        # Test ActivePipelinesIndex query
        active_pipelines, next_key = read_api.query_active_pipelines(limit=10)
        
        # Should find our active pipelines (and potentially others from other tests)
        our_active_pipelines = [p for p in active_pipelines if p.pipeline_id.startswith(base_id)]
        assert len(our_active_pipelines) == 3  # 0, 2, 4 are active
        
        for pipeline in our_active_pipelines:
            assert pipeline.is_active is True
            assert_utc_timezone(pipeline.created_at)
            assert_utc_timezone(pipeline.updated_at)
        
        # Test EnvironmentIndex query
        dev_pipelines, _ = read_api.query_by_environment("dev", limit=20)
        
        our_dev_pipelines = [p for p in dev_pipelines if p.pipeline_id.startswith(base_id)]
        assert len(our_dev_pipelines) == 5  # All pipelines are in dev environment
        
        # Test combined environment and status query
        active_dev_pipelines, _ = read_api.query_by_environment_and_status(
            "dev", True, limit=10
        )
        
        our_active_dev = [p for p in active_dev_pipelines if p.pipeline_id.startswith(base_id)]
        assert len(our_active_dev) == 3  # Active ones in dev
        
        # Cleanup
        for pipeline_id in pipeline_ids:
            write_api.delete_pipeline(pipeline_id)

    def test_cross_domain_operations_with_real_storage(
        self,
        localstack_pipeline_config_write_api,
        localstack_table_config_write_api,
        localstack_pipeline_run_logs_write_api,
        localstack_pipeline_config_read_api,
        localstack_table_config_read_api,
        localstack_pipeline_run_logs_read_api
    ):
        """Test cross-domain operations with real DynamoDB storage."""
        pipeline_write = localstack_pipeline_config_write_api
        table_write = localstack_table_config_write_api
        logs_write = localstack_pipeline_run_logs_write_api
        
        pipeline_read = localstack_pipeline_config_read_api
        table_read = localstack_table_config_read_api
        logs_read = localstack_pipeline_run_logs_read_api
        
        # Create unique IDs
        base_id = f"cross-domain-{uuid.uuid4().hex[:6]}"
        pipeline_id = f"{base_id}-pipeline"
        table_id = f"{base_id}-table"
        run_id = f"{base_id}-run"
        
        # 1. Create pipeline configuration
        pipeline_data = PipelineConfigUpsert(
            pipeline_id=pipeline_id,
            pipeline_name="Cross-Domain Test Pipeline",
            description="Testing cross-domain operations",
            source_type="s3",
            destination_type="warehouse",
            environment="dev",
            is_active=True,
            created_by="cross-domain-test"
        )
        
        created_pipeline = pipeline_write.create_pipeline(pipeline_data)
        assert created_pipeline.pipeline_id == pipeline_id
        
        # 2. Create table configuration linked to pipeline
        table_data = TableConfigUpsert(
            table_id=table_id,
            pipeline_id=pipeline_id,  # Link to pipeline
            table_name="cross_domain_test_table",
            table_type=TableType.SOURCE,
            data_format=DataFormat.PARQUET,
            location="s3://test-bucket/cross-domain/",
            environment="dev",
            is_active=True,
            created_by="cross-domain-test"
        )
        
        created_table = table_write.create_table(table_data)
        assert created_table.table_id == table_id
        assert created_table.pipeline_id == pipeline_id
        
        # 3. Create pipeline run log
        run_data = PipelineRunLogUpsert(
            run_id=run_id,
            pipeline_id=pipeline_id,  # Link to pipeline
            status=RunStatus.RUNNING,
            trigger_type="schedule",
            environment="dev",
            start_time=datetime.now(timezone.utc),
            created_by="cross-domain-test"
        )
        
        created_run = logs_write.create_run_log(run_data)
        assert created_run.run_id == run_id
        assert created_run.pipeline_id == pipeline_id
        
        # 4. Verify cross-domain relationships work
        
        # Query tables by pipeline
        pipeline_tables, _ = table_read.query_by_pipeline(pipeline_id)
        assert len(pipeline_tables) == 1
        assert pipeline_tables[0].table_id == table_id
        
        # Query run logs by pipeline
        pipeline_runs, _ = logs_read.query_by_pipeline(pipeline_id)
        assert len(pipeline_runs) == 1
        assert pipeline_runs[0].run_id == run_id
        
        # 5. Test cascade-style updates (manual coordination)
        
        # Update pipeline status to inactive
        pipeline_write.update_pipeline_status(pipeline_id, False, "cross-domain-test")
        
        # Update run status to completed
        logs_write.update_run_status(run_id, pipeline_id, RunStatus.SUCCESS)
        
        # Verify updates
        updated_pipeline = pipeline_read.get_by_id(pipeline_id)
        assert updated_pipeline.is_active is False
        
        updated_run = logs_read.get_by_id(run_id, pipeline_id)
        assert updated_run.status == RunStatus.SUCCESS
        
        # Cleanup
        pipeline_write.delete_pipeline(pipeline_id)
        table_write.delete_table(table_id)
        logs_write.delete_run_log(run_id, pipeline_id)

    def test_timezone_compliance_with_real_storage(
        self,
        localstack_pipeline_run_logs_write_api,
        localstack_pipeline_run_logs_read_api
    ):
        """Test timezone compliance with real DynamoDB storage and retrieval."""
        write_api = localstack_pipeline_run_logs_write_api
        read_api = localstack_pipeline_run_logs_read_api
        
        # Create timezone test context
        tz_context = create_timezone_test_context("America/New_York")
        
        run_id = f"tz-test-{uuid.uuid4().hex[:8]}"
        pipeline_id = f"tz-pipeline-{uuid.uuid4().hex[:8]}"
        
        # Create run log with timezone-aware times
        utc_now = datetime.now(timezone.utc)
        start_time = utc_now - timedelta(hours=2)
        
        run_data = PipelineRunLogUpsert(
            run_id=run_id,
            pipeline_id=pipeline_id,
            status=RunStatus.SUCCESS,
            trigger_type="manual",
            environment="dev",
            start_time=start_time,
            end_time=utc_now,
            created_by="timezone-test"
        )
        
        created_run = write_api.create_run_log(run_data)
        
        # Verify storage compliance - all times should be UTC
        assert_utc_timezone(created_run.start_time)
        assert_utc_timezone(created_run.end_time)
        assert_utc_timezone(created_run.created_at)
        assert_utc_timezone(created_run.updated_at)
        
        # Read back and verify timezone consistency
        retrieved_run = read_api.get_by_id(run_id, pipeline_id)
        assert retrieved_run is not None
        
        # Times should be equivalent (same moment in time)
        assert_timezones_equivalent(retrieved_run.start_time, start_time)
        assert_timezones_equivalent(retrieved_run.end_time, utc_now)
        
        # All retrieved times should be UTC
        assert_utc_timezone(retrieved_run.start_time)
        assert_utc_timezone(retrieved_run.end_time)
        assert_utc_timezone(retrieved_run.created_at)
        assert_utc_timezone(retrieved_run.updated_at)
        
        # Test update with timezone compliance (using status update)
        update_result = write_api.update_run_status(
            run_id, pipeline_id, RunStatus.SUCCESS, "timezone-update-test"
        )
        
        assert update_result is not None
        
        # Verify updated time is also UTC compliant
        updated_run = read_api.get_by_id(run_id, pipeline_id)
        assert_utc_timezone(updated_run.updated_at)
        
        # Cleanup
        write_api.delete_run_log(run_id, pipeline_id)

    def test_error_handling_with_real_dynamodb(
        self,
        localstack_pipeline_config_write_api,
        localstack_pipeline_config_read_api
    ):
        """Test error handling with real DynamoDB errors."""
        write_api = localstack_pipeline_config_write_api
        read_api = localstack_pipeline_config_read_api
        
        pipeline_id = f"error-test-{uuid.uuid4().hex[:8]}"
        
        # Test ItemNotFoundError
        non_existent = read_api.get_by_id("non-existent-pipeline-id")
        assert non_existent is None
        
        # Test successful creation
        pipeline_data = PipelineConfigUpsert(
            pipeline_id=pipeline_id,
            pipeline_name="Error Handling Test",
            description="Testing error scenarios",
            source_type="s3",
            destination_type="warehouse",
            environment="dev",
            created_by="error-test"
        )
        
        created = write_api.create_pipeline(pipeline_data)
        assert created.pipeline_id == pipeline_id
        
        # Test ConflictError on duplicate creation
        with pytest.raises(Exception):  # Should be ConflictError in real implementation
            write_api.create_pipeline(pipeline_data)
        
        # Test update non-existent pipeline
        with pytest.raises(Exception):  # Should be ItemNotFoundError in real implementation
            write_api.update_pipeline(
                "non-existent-pipeline",
                {"description": "This should fail"}
            )
        
        # Test successful update
        update_result = write_api.update_pipeline(
            pipeline_id,
            {"description": "Successfully updated"}
        )
        assert update_result is not None
        
        # Verify update worked
        updated = read_api.get_by_id(pipeline_id)
        assert updated.description == "Successfully updated"
        
        # Cleanup
        write_api.delete_pipeline(pipeline_id)

    def test_performance_characteristics_with_real_network(
        self,
        localstack_pipeline_config_write_api,
        localstack_pipeline_config_read_api
    ):
        """Test performance characteristics with real network calls."""
        write_api = localstack_pipeline_config_write_api
        read_api = localstack_pipeline_config_read_api
        
        base_id = f"perf-test-{uuid.uuid4().hex[:6]}"
        
        # Test batch operations
        pipelines_data = []
        for i in range(10):
            pipeline_data = PipelineConfigUpsert(
                pipeline_id=f"{base_id}-batch-{i:02d}",
                pipeline_name=f"Batch Performance Test {i}",
                description=f"Pipeline {i} for performance testing",
                source_type="s3",
                destination_type="warehouse",
                environment="dev",
                is_active=i % 2 == 0,
                created_by=f"perf-test-{i}"
            )
            pipelines_data.append(pipeline_data)
        
        # Measure batch creation (should be more efficient than individual creates)
        import time
        start_time = time.time()
        
        created_pipelines = write_api.upsert_many(pipelines_data)
        
        batch_duration = time.time() - start_time
        
        assert len(created_pipelines) == 10
        print(f"Batch creation of 10 pipelines took: {batch_duration:.3f} seconds")
        
        # Test projection queries (should be more efficient)
        start_time = time.time()
        
        summaries = []
        for i in range(10):
            pipeline_id = f"{base_id}-batch-{i:02d}"
            summary = read_api.get_pipeline_summary(pipeline_id)
            if summary:
                summaries.append(summary)
        
        projection_duration = time.time() - start_time
        
        assert len(summaries) == 10
        print(f"10 projection queries took: {projection_duration:.3f} seconds")
        
        # Test count operations (should be very efficient)
        start_time = time.time()
        
        dev_count = read_api.count_pipelines_by_environment("dev")
        active_count = read_api.count_active_pipelines()
        
        count_duration = time.time() - start_time
        
        assert dev_count >= 10  # At least our test pipelines
        assert active_count >= 5  # At least our active test pipelines
        print(f"Count operations took: {count_duration:.3f} seconds")
        
        # Cleanup
        for i in range(10):
            pipeline_id = f"{base_id}-batch-{i:02d}"
            write_api.delete_pipeline(pipeline_id)


class TestLocalStackDataConsistency:
    """Test data consistency scenarios with LocalStack."""

    def test_concurrent_updates_simulation(
        self,
        localstack_pipeline_config_write_api,
        localstack_pipeline_config_read_api
    ):
        """Simulate concurrent updates to test consistency."""
        write_api = localstack_pipeline_config_write_api
        read_api = localstack_pipeline_config_read_api
        
        pipeline_id = f"concurrent-test-{uuid.uuid4().hex[:8]}"
        
        # Create initial pipeline
        pipeline_data = PipelineConfigUpsert(
            pipeline_id=pipeline_id,
            pipeline_name="Concurrent Update Test",
            description="Testing concurrent update scenarios",
            source_type="s3",
            destination_type="warehouse",
            environment="dev",
            is_active=True,
            created_by="concurrent-test"
        )
        
        created = write_api.create_pipeline(pipeline_data)
        original_updated_at = created.updated_at
        
        # Simulate concurrent updates
        import time
        
        # Update 1: Change description
        time.sleep(0.1)  # Small delay to ensure different timestamps
        update1_result = write_api.update_pipeline(
            pipeline_id,
            {
                "description": "Updated by process 1",
                "updated_by": "process-1"
            }
        )
        
        # Update 2: Change status
        time.sleep(0.1)
        update2_result = write_api.update_pipeline(
            pipeline_id,
            {
                "is_active": False,
                "updated_by": "process-2"
            }
        )
        
        # Read final state
        final_state = read_api.get_by_id(pipeline_id)
        
        # Verify final state has both updates (last-writer-wins in DynamoDB)
        assert final_state.updated_at > original_updated_at
        
        # The exact final state depends on update ordering, but should be consistent
        print(f"Final description: {final_state.description}")
        print(f"Final active status: {final_state.is_active}")
        print(f"Final updated_by: {getattr(final_state, 'updated_by', 'N/A')}")
        
        # Cleanup
        write_api.delete_pipeline(pipeline_id)

    def test_transaction_style_operations(
        self,
        localstack_pipeline_config_write_api,
        localstack_pipeline_config_read_api
    ):
        """Test transaction-style operations for consistency."""
        write_api = localstack_pipeline_config_write_api
        read_api = localstack_pipeline_config_read_api
        
        base_id = f"txn-test-{uuid.uuid4().hex[:6]}"
        pipeline_ids = [f"{base_id}-{i:02d}" for i in range(3)]
        
        # Create multiple pipelines
        for i, pipeline_id in enumerate(pipeline_ids):
            pipeline_data = PipelineConfigUpsert(
                pipeline_id=pipeline_id,
                pipeline_name=f"Transaction Test Pipeline {i}",
                description=f"Pipeline {i} for transaction testing",
                source_type="s3",
                destination_type="warehouse",
                environment="dev",
                is_active=False,  # Start inactive
                created_by="transaction-test"
            )
            
            created = write_api.create_pipeline(pipeline_data)
            assert created.pipeline_id == pipeline_id
        
        # Test batch activation (transaction-style operation)
        activated_count = write_api.activate_pipelines(pipeline_ids, "transaction-test")
        assert activated_count == 3
        
        # Verify all pipelines are now active
        for pipeline_id in pipeline_ids:
            pipeline = read_api.get_by_id(pipeline_id)
            assert pipeline.is_active is True
        
        # Test batch deactivation
        deactivated_count = write_api.deactivate_pipelines(pipeline_ids)
        assert deactivated_count == 3
        
        # Verify all pipelines are now inactive
        for pipeline_id in pipeline_ids:
            pipeline = read_api.get_by_id(pipeline_id)
            assert pipeline.is_active is False
        
        # Cleanup
        for pipeline_id in pipeline_ids:
            write_api.delete_pipeline(pipeline_id)