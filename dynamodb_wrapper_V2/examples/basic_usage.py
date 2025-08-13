#!/usr/bin/env python3
"""
Basic usage examples for the DynamoDB wrapper library V2.

This example demonstrates the new CQRS architecture:
1. Setting up configuration
2. Using separate read/write APIs for pipeline configurations
3. Using separate read/write APIs for table configurations
4. Using separate read/write APIs for pipeline run logs
5. Optimized projections and validated DTOs
"""

from datetime import datetime
from decimal import Decimal

from dynamodb_wrapper import (
    DynamoDBConfig,
    # Pipeline Config CQRS APIs
    PipelineConfigReadApi,
    PipelineConfigWriteApi,
    PipelineConfigUpsert,
    # Table Config CQRS APIs  
    TableConfigReadApi,
    TableConfigWriteApi,
    TableConfigUpsert,
    # Pipeline Run Logs CQRS APIs
    PipelineRunLogsReadApi,
    PipelineRunLogsWriteApi,
    PipelineRunLogUpsert,
    PipelineRunLogStatusUpdate,
)
from dynamodb_wrapper.models.domain_models import RunStatus, DataFormat, TableType


def main():
    """Demonstrate basic usage of the CQRS DynamoDB wrapper."""

    # 1. Configure DynamoDB connection
    print("1. Setting up DynamoDB configuration...")
    config = DynamoDBConfig.from_env()  # Uses environment variables

    # For local development, you might use:
    # config = DynamoDBConfig.for_local_development()

    # 2. Initialize CQRS APIs (separate read/write)
    print("2. Initializing CQRS APIs...")
    pipeline_read_api = PipelineConfigReadApi(config)
    pipeline_write_api = PipelineConfigWriteApi(config)
    table_read_api = TableConfigReadApi(config)
    table_write_api = TableConfigWriteApi(config)
    logs_read_api = PipelineRunLogsReadApi(config)
    logs_write_api = PipelineRunLogsWriteApi(config)

    # 3. Create a pipeline configuration using validated DTO
    print("3. Creating pipeline configuration...")
    pipeline_dto = PipelineConfigUpsert(
        pipeline_id="sales-analytics-pipeline",
        pipeline_name="Sales Analytics Pipeline",
        description="Daily sales data processing pipeline",
        source_type="s3",
        destination_type="warehouse",  # Updated to valid type
        schedule_expression="0 9 * * *",  # Daily at 9 AM
        environment="dev",
        spark_config={
            "spark.sql.adaptive.enabled": "true",
            "spark.executor.memory": "4g"
        },
        cpu_cores=4,
        memory_gb=Decimal('8.0'),
        tags={"team": "analytics", "project": "sales"},
        created_by="data_engineer"
    )
    pipeline_config = pipeline_write_api.create_pipeline(pipeline_dto)
    print(f"Created pipeline: {pipeline_config.pipeline_id}")

    # 4. Create table configurations using validated DTOs
    print("4. Creating table configurations...")

    # Source table configuration
    source_dto = TableConfigUpsert(
        table_id="sales-raw-data",
        pipeline_id=pipeline_config.pipeline_id,
        table_name="sales_raw",
        table_type=TableType.SOURCE,
        data_format=DataFormat.JSON,
        location="s3://my-bucket/raw-sales/",
        schema_definition={
            "fields": [
                {"name": "transaction_id", "type": "string"},
                {"name": "customer_id", "type": "string"},
                {"name": "amount", "type": "decimal"},
                {"name": "timestamp", "type": "timestamp"}
            ]
        },
        partition_columns=["date"],
        read_options={
            "multiline": True,
            "inferSchema": False
        },
        created_by="data_engineer"
    )
    source_table = table_write_api.create_table(source_dto)

    # Destination table configuration
    dest_dto = TableConfigUpsert(
        table_id="sales-processed-data",
        pipeline_id=pipeline_config.pipeline_id,
        table_name="sales_processed",
        table_type=TableType.DESTINATION,
        data_format=DataFormat.PARQUET,
        location="s3://my-bucket/processed-sales/",
        partition_columns=["year", "month"],
        write_options={
            "compression": "snappy"
        },
        created_by="data_engineer"
    )
    dest_table = table_write_api.create_table(dest_dto)

    print(f"Created source table: {source_table.table_id}")
    print(f"Created destination table: {dest_table.table_id}")

    # 5. Create a pipeline run log using validated DTO
    print("5. Creating pipeline run log...")
    run_dto = PipelineRunLogUpsert(
        run_id="run-20241210-001",
        pipeline_id=pipeline_config.pipeline_id,
        trigger_type="schedule",
        status=RunStatus.PENDING,
        created_by="scheduler"
    )
    run_log = logs_write_api.create_run_log(run_dto)
    print(f"Created run log: {run_log.run_id}")

    # 6. Update run status as pipeline progresses
    print("6. Updating pipeline run status...")

    # Start the run using status update DTO
    start_update = PipelineRunLogStatusUpdate(
        status=RunStatus.RUNNING
    )
    logs_write_api.update_run_status_with_dto(run_log.run_id, start_update)

    # Simulate pipeline completion
    end_time = datetime.now()
    completion_update = PipelineRunLogStatusUpdate(
        status=RunStatus.SUCCESS,
        end_time=end_time
    )
    logs_write_api.update_run_status_with_dto(run_log.run_id, completion_update)
    print("Pipeline run completed successfully")

    # 7. Query examples (using optimized read APIs)
    print("7. Querying data with optimized projections...")

    # Get pipeline configuration (returns optimized view model)
    retrieved_pipeline = pipeline_read_api.get_by_id(pipeline_config.pipeline_id)
    print(f"Retrieved pipeline: {retrieved_pipeline.pipeline_name}")

    # Get pipeline summary (minimal projection)
    pipeline_summary = pipeline_read_api.get_pipeline_summary(pipeline_config.pipeline_id)
    print(f"Pipeline summary active status: {pipeline_summary.is_active}")

    # Get active pipelines with custom projection (reduces RCUs)
    active_pipelines, _ = pipeline_read_api.query_active_pipelines(
        projection=['pipeline_id', 'pipeline_name', 'environment'],
        limit=10
    )
    print(f"Active pipelines: {len(active_pipelines)}")

    # Get tables for pipeline
    pipeline_tables, _ = table_read_api.query_by_pipeline(
        pipeline_config.pipeline_id,
        projection=['table_id', 'table_name', 'table_type']
    )
    print(f"Tables for pipeline: {len(pipeline_tables)}")

    # Get recent runs with minimal projection
    recent_runs, _ = logs_read_api.query_by_pipeline(
        pipeline_config.pipeline_id,
        projection=['run_id', 'status', 'start_time'],
        limit=10
    )
    print(f"Recent runs: {len(recent_runs)}")

    # 8. Update examples (using write APIs)
    print("8. Updating configurations...")

    # Update pipeline status
    pipeline_write_api.update_pipeline_status(
        pipeline_config.pipeline_id,
        is_active=False,
        updated_by="admin"
    )
    print("Pipeline deactivated")

    # Update table statistics
    table_write_api.update_table_statistics(
        source_table.table_id,
        record_count=10000,
        size_bytes=5000000,
        last_updated_data=datetime.now()
    )
    print("Table statistics updated")

    print("\n✅ V2 CQRS Architecture Example Completed!")
    print("\nKey V2 Features Demonstrated:")
    print("• Separate Read/Write APIs for optimized operations")
    print("• Validated DTOs with comprehensive business rules")
    print("• Optimized view models with reduced payload sizes")
    print("• Custom projections to minimize RCU consumption")
    print("• Safe handling of DynamoDB reserved words")
    print("• Pagination support with proper last_key handling")


if __name__ == "__main__":
    main()
