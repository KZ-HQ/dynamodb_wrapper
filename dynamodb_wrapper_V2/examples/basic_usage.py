#!/usr/bin/env python3
"""
Basic usage examples for the DynamoDB wrapper library.

This example demonstrates:
1. Setting up configuration
2. Creating and managing pipeline configurations
3. Creating and managing table configurations
4. Creating and tracking pipeline run logs
5. Basic CRUD operations
"""

from datetime import datetime
from decimal import Decimal

from dynamodb_wrapper_V2.dynamodb_wrapper import (
    DynamoDBConfig,
    PipelineConfigRepository,
    PipelineRunLogsRepository,
    TableConfigRepository,
)
from dynamodb_wrapper_V2.dynamodb_wrapper.models.pipeline_run_log import RunStatus
from dynamodb_wrapper_V2.dynamodb_wrapper.models.table_config import DataFormat, TableType


def main():
    """Demonstrate basic usage of the DynamoDB wrapper."""

    # 1. Configure DynamoDB connection
    print("1. Setting up DynamoDB configuration...")
    config = DynamoDBConfig.from_env()  # Uses environment variables

    # For local development, you might use:
    # config = DynamoDBConfig.for_local_development()

    # 2. Initialize repositories
    print("2. Initializing repositories...")
    pipeline_repo = PipelineConfigRepository(config)
    table_repo = TableConfigRepository(config)
    logs_repo = PipelineRunLogsRepository(config)

    # 3. Create a pipeline configuration
    print("3. Creating pipeline configuration...")
    pipeline_config = pipeline_repo.create_pipeline_config(
        pipeline_id="sales-analytics-pipeline",
        pipeline_name="Sales Analytics Pipeline",
        description="Daily sales data processing pipeline",
        source_type="s3",
        destination_type="redshift",
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
    print(f"Created pipeline: {pipeline_config.pipeline_id}")

    # 4. Create table configurations
    print("4. Creating table configurations...")

    # Source table configuration
    source_table = table_repo.create_table_config(
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
        }
    )

    # Destination table configuration
    dest_table = table_repo.create_table_config(
        table_id="sales-processed-data",
        pipeline_id=pipeline_config.pipeline_id,
        table_name="sales_processed",
        table_type=TableType.DESTINATION,
        data_format=DataFormat.PARQUET,
        location="s3://my-bucket/processed-sales/",
        partition_columns=["year", "month"],
        write_options={
            "compression": "snappy"
        }
    )

    print(f"Created source table: {source_table.table_id}")
    print(f"Created destination table: {dest_table.table_id}")

    # 5. Create a pipeline run log
    print("5. Creating pipeline run log...")
    run_log = logs_repo.create_run_log(
        run_id="run-20241210-001",
        pipeline_id=pipeline_config.pipeline_id,
        trigger_type="schedule",
        created_by="scheduler"
    )
    print(f"Created run log: {run_log.run_id}")

    # 6. Update run status as pipeline progresses
    print("6. Updating pipeline run status...")

    # Start the run
    logs_repo.update_run_status(run_log.run_id, RunStatus.RUNNING)

    # Simulate pipeline completion
    end_time = datetime.now()
    logs_repo.update_run_status(
        run_log.run_id,
        RunStatus.SUCCESS,
        end_time=end_time
    )
    print("Pipeline run completed successfully")

    # 7. Query examples
    print("7. Querying data...")

    # Get pipeline configuration
    retrieved_pipeline = pipeline_repo.get_by_pipeline_id(pipeline_config.pipeline_id)
    print(f"Retrieved pipeline: {retrieved_pipeline.pipeline_name}")

    # Get all active pipelines
    active_pipelines = pipeline_repo.get_active_pipelines()
    print(f"Active pipelines: {len(active_pipelines)}")

    # Get tables for pipeline
    pipeline_tables = table_repo.get_tables_by_pipeline(pipeline_config.pipeline_id)
    print(f"Tables for pipeline: {len(pipeline_tables)}")

    # Get recent runs
    recent_runs = logs_repo.get_runs_by_pipeline(pipeline_config.pipeline_id, limit=10)
    print(f"Recent runs: {len(recent_runs)}")

    # 8. Update examples
    print("8. Updating configurations...")

    # Update pipeline status
    pipeline_repo.update_pipeline_status(
        pipeline_config.pipeline_id,
        is_active=False,
        updated_by="admin"
    )
    print("Pipeline deactivated")

    # Update table statistics
    table_repo.update_table_statistics(
        source_table.table_id,
        record_count=10000,
        size_bytes=5000000,
        last_updated_data=datetime.now()
    )
    print("Table statistics updated")

    print("\nBasic usage example completed successfully!")


if __name__ == "__main__":
    main()
