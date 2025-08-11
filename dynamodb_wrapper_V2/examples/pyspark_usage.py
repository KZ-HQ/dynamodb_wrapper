#!/usr/bin/env python3
"""
PySpark integration examples for the DynamoDB wrapper library.

This example demonstrates:
1. Setting up DynamoDB configuration for PySpark
2. Creating Spark session with pipeline configuration
3. Using pipeline run context manager
4. Reading and writing data with table configurations
5. Automatic logging and monitoring
"""

from dynamodb_wrapper_V2.dynamodb_wrapper import (
    DynamoDBConfig,
    PipelineConfigRepository,
    TableConfigRepository,
)
from dynamodb_wrapper_V2.dynamodb_wrapper.models.table_config import TableType
from dynamodb_wrapper_V2.dynamodb_wrapper.utils import (
    SparkDynamoDBIntegration,
    create_spark_session_with_dynamodb,
    get_pipeline_config_for_spark,
    get_table_configs_for_spark,
)


def setup_test_configuration():
    """Set up test pipeline and table configurations."""
    # Use PySpark optimized configuration
    config = DynamoDBConfig.for_pyspark()

    pipeline_repo = PipelineConfigRepository(config)
    table_repo = TableConfigRepository(config)

    # Create or get existing pipeline configuration
    pipeline_id = "spark-etl-pipeline"

    try:
        pipeline_config = pipeline_repo.get_by_pipeline_id(pipeline_id)
        if not pipeline_config:
            raise Exception("Pipeline not found")
    except:
        # Create new pipeline configuration
        pipeline_config = pipeline_repo.create_pipeline_config(
            pipeline_id=pipeline_id,
            pipeline_name="Spark ETL Pipeline",
            description="PySpark ETL pipeline example",
            source_type="s3",
            destination_type="s3",
            spark_config={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.executor.memory": "4g",
                "spark.executor.cores": "2",
                "spark.sql.execution.arrow.pyspark.enabled": "true"
            },
            cpu_cores=4,
            memory_gb=8.0,
            created_by="spark_user"
        )

    # Create table configurations
    try:
        source_tables = table_repo.get_source_tables(pipeline_id)
        if not source_tables:
            raise Exception("No source tables")
    except:
        # Create source table
        table_repo.create_table_config(
            table_id="customer-transactions",
            pipeline_id=pipeline_id,
            table_name="customer_transactions",
            table_type=TableType.SOURCE,
            data_format="parquet",
            location="s3://my-data-bucket/input/customer-transactions/",
            partition_columns=["year", "month"],
            read_options={
                "mergeSchema": "true"
            }
        )

        # Create destination table
        table_repo.create_table_config(
            table_id="customer-summary",
            pipeline_id=pipeline_id,
            table_name="customer_summary",
            table_type=TableType.DESTINATION,
            data_format="parquet",
            location="s3://my-data-bucket/output/customer-summary/",
            partition_columns=["processing_date"],
            write_options={
                "compression": "snappy",
                "mode": "overwrite"
            }
        )

    return pipeline_id, config


def example_basic_spark_integration():
    """Example of basic Spark integration."""
    print("=== Basic Spark Integration Example ===")

    pipeline_id, config = setup_test_configuration()

    # Method 1: Create Spark session directly
    spark = create_spark_session_with_dynamodb(
        app_name="Customer Analytics",
        pipeline_id=pipeline_id,
        config=config,
        additional_config={
            "spark.sql.warehouse.dir": "/tmp/spark-warehouse"
        }
    )

    print(f"Created Spark session: {spark.sparkContext.appName}")

    # Get pipeline configuration
    pipeline_config = get_pipeline_config_for_spark(pipeline_id, config)
    print(f"Pipeline: {pipeline_config.pipeline_name}")

    # Get table configurations
    table_configs = get_table_configs_for_spark(pipeline_id, config)
    print(f"Found {len(table_configs)} table configurations")

    spark.stop()


def example_advanced_integration():
    """Example of advanced integration with context manager."""
    print("\n=== Advanced Integration with Context Manager ===")

    pipeline_id, config = setup_test_configuration()

    # Initialize integration
    integration = SparkDynamoDBIntegration(config)

    # Use pipeline run context manager
    with integration.pipeline_run_context(
        pipeline_id=pipeline_id,
        trigger_type="manual",
        created_by="data_scientist"
    ) as run_id:

        print(f"Started pipeline run: {run_id}")

        # Create Spark session
        spark = integration.create_spark_session(
            pipeline_id,
            additional_config={
                "spark.sql.warehouse.dir": "/tmp/spark-warehouse"
            }
        )

        try:
            # Get table configurations
            source_tables = integration.table_repo.get_source_tables(pipeline_id)
            dest_tables = integration.table_repo.get_destination_tables(pipeline_id)

            print(f"Processing {len(source_tables)} source tables")
            print(f"Writing to {len(dest_tables)} destination tables")

            # Process each source table
            for source_table in source_tables:
                print(f"\nProcessing table: {source_table.table_name}")

                # Get read options from table configuration
                read_options = integration.get_table_read_options(source_table.table_id)
                print(f"Read options: {read_options}")

                # Simulate reading data (would normally read from actual source)
                # df = spark.read.options(**read_options).load()

                # For demo, create sample DataFrame
                sample_data = [
                    ("cust_001", "2024-01-15", 150.00, "electronics"),
                    ("cust_002", "2024-01-15", 75.50, "books"),
                    ("cust_003", "2024-01-15", 220.25, "clothing"),
                ]

                df = spark.createDataFrame(
                    sample_data,
                    ["customer_id", "transaction_date", "amount", "category"]
                )

                print(f"Created sample DataFrame with {df.count()} rows")

                # Process data (example aggregation)
                summary_df = df.groupBy("customer_id").agg(
                    spark.sql.functions.sum("amount").alias("total_amount"),
                    spark.sql.functions.count("*").alias("transaction_count"),
                    spark.sql.functions.max("transaction_date").alias("last_transaction_date")
                ).withColumn("processing_date", spark.sql.functions.current_date())

                print(f"Created summary with {summary_df.count()} rows")

                # Write to destination
                for dest_table in dest_tables:
                    print(f"Writing to: {dest_table.table_name}")

                    # Get write options from table configuration
                    write_options = integration.get_table_write_options(dest_table.table_id)
                    print(f"Write options: {write_options}")

                    # Simulate writing (would normally write to actual destination)
                    # summary_df.write.options(**write_options).save()

                    # Update table statistics
                    integration.update_table_stats_after_write(
                        dest_table.table_id,
                        summary_df,
                        run_id
                    )

                    print("Table statistics updated")

            print(f"\nPipeline run {run_id} completed successfully")

        finally:
            spark.stop()


def example_error_handling():
    """Example of error handling in pipeline runs."""
    print("\n=== Error Handling Example ===")

    pipeline_id, config = setup_test_configuration()
    integration = SparkDynamoDBIntegration(config)

    # Demonstrate error handling with context manager
    try:
        with integration.pipeline_run_context(
            pipeline_id=pipeline_id,
            trigger_type="manual",
            created_by="test_user"
        ) as run_id:

            print(f"Started pipeline run: {run_id}")

            # Simulate an error
            raise ValueError("Simulated processing error")

    except ValueError as e:
        print(f"Pipeline failed with error: {e}")

        # The context manager automatically logs the failure
        # Check the run log
        run_log = integration.logs_repo.get_by_run_id(run_id)
        if run_log:
            print(f"Run status: {run_log.status}")
            print(f"Error message: {run_log.error_message}")


def example_monitoring_and_stats():
    """Example of monitoring and statistics collection."""
    print("\n=== Monitoring and Statistics Example ===")

    pipeline_id, config = setup_test_configuration()
    integration = SparkDynamoDBIntegration(config)

    # Get pipeline statistics
    recent_runs = integration.logs_repo.get_recent_runs(pipeline_id, hours=24)
    print(f"Recent runs (24h): {len(recent_runs)}")

    failed_runs = integration.logs_repo.get_failed_runs(pipeline_id, hours=24)
    print(f"Failed runs (24h): {len(failed_runs)}")

    running_pipelines = integration.logs_repo.get_running_pipelines()
    print(f"Currently running pipelines: {len(running_pipelines)}")

    # Get table statistics
    tables = integration.table_repo.get_tables_by_pipeline(pipeline_id)
    for table in tables:
        print(f"\nTable: {table.table_name}")
        print(f"  Type: {table.table_type}")
        print(f"  Format: {table.data_format}")
        print(f"  Records: {table.record_count or 'N/A'}")
        print(f"  Size: {table.size_bytes or 'N/A'} bytes")
        print(f"  Last updated: {table.last_updated_data or 'N/A'}")


def main():
    """Run all PySpark examples."""
    print("PySpark Integration Examples\n")

    try:
        example_basic_spark_integration()
        example_advanced_integration()
        example_error_handling()
        example_monitoring_and_stats()

        print("\n=== All examples completed successfully! ===")

    except Exception as e:
        print(f"\nExample failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
