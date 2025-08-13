#!/usr/bin/env python3
"""
DynamoDB wrapper usage examples with CQRS architecture.

This example demonstrates:
1. Setting up DynamoDB configuration
2. Using CQRS APIs (Read/Write separation)
3. Working with pipeline configurations
4. Managing table configurations
5. Pipeline run logging with model-driven key management
6. Best practices for data processing pipelines

Note: PySpark integration has been removed from V2. This example focuses on
the core CQRS architecture and model-driven key management.
"""

from datetime import datetime
from dynamodb_wrapper import DynamoDBConfig
from dynamodb_wrapper.pipeline_config.queries import PipelineConfigReadApi
from dynamodb_wrapper.pipeline_config.commands import PipelineConfigWriteApi
from dynamodb_wrapper.table_config.queries import TableConfigReadApi
from dynamodb_wrapper.table_config.commands import TableConfigWriteApi
from dynamodb_wrapper.pipeline_run_logs.queries import PipelineRunLogsReadApi
from dynamodb_wrapper.pipeline_run_logs.commands import PipelineRunLogsWriteApi
from dynamodb_wrapper.models import (
    PipelineConfigUpsert,
    TableConfigUpsert,
    PipelineRunLogUpsert,
    TableType,
    DataFormat,
    RunStatus,
)


def setup_configuration():
    """Set up DynamoDB configuration for data processing."""
    print("=== Setting up DynamoDB Configuration ===")
    
    config = DynamoDBConfig(
        region_name="us-east-1",
        table_prefix="data-pipeline",
        environment="dev",
        # Add any additional configuration needed
    )
    
    print(f"Region: {config.region_name}")
    print(f"Environment: {config.environment}")
    print(f"Table prefix: {config.table_prefix}")
    
    return config


def demonstrate_pipeline_configuration(config):
    """Demonstrate pipeline configuration management with CQRS."""
    print("\n=== Pipeline Configuration Management ===")
    
    # Initialize CQRS APIs
    write_api = PipelineConfigWriteApi(config)
    read_api = PipelineConfigReadApi(config)
    
    # Create pipeline configuration
    pipeline_data = PipelineConfigUpsert(
        pipeline_id="data-processing-pipeline-v1",
        pipeline_name="Data Processing Pipeline V1",
        description="Processes customer data from S3 to data warehouse",
        source_type="s3",
        source_config={
            "bucket": "customer-data-bucket",
            "prefix": "raw-data/",
            "format": "parquet"
        },
        destination_type="redshift",
        destination_config={
            "cluster": "data-warehouse-cluster",
            "database": "analytics",
            "schema": "customer"
        },
        environment="dev",
        schedule_expression="cron(0 2 * * ? *)",  # Daily at 2 AM
        is_active=True,
        # CPU and memory requirements
        cpu_cores=4,
        memory_gb=16,
        tags={
            "team": "data-engineering",
            "priority": "high",
            "cost-center": "analytics"
        }
    )
    
    print(f"Creating pipeline: {pipeline_data.pipeline_id}")
    print(f"Description: {pipeline_data.description}")
    print(f"Source: {pipeline_data.source_type}")
    print(f"Destination: {pipeline_data.destination_type}")
    print(f"Schedule: {pipeline_data.schedule_expression}")
    
    try:
        # Note: In real usage, this would create the pipeline in DynamoDB
        print("✓ Pipeline configuration would be created successfully")
        return pipeline_data.pipeline_id
    except Exception as e:
        print(f"Demo note: {e} (Expected in demo environment)")
        return pipeline_data.pipeline_id


def demonstrate_table_configuration(config, pipeline_id):
    """Demonstrate table configuration management."""
    print("\n=== Table Configuration Management ===")
    
    # Initialize table config APIs
    write_api = TableConfigWriteApi(config)
    read_api = TableConfigReadApi(config)
    
    # Create source table configuration
    source_table = TableConfigUpsert(
        table_id="customer-data-source",
        pipeline_id=pipeline_id,
        table_name="customer_raw_data",
        table_type=TableType.SOURCE,
        data_format=DataFormat.PARQUET,
        location="s3://customer-data-bucket/raw-data/",
        schema_definition={
            "customer_id": {"type": "string", "nullable": False},
            "email": {"type": "string", "nullable": False},
            "created_at": {"type": "timestamp", "nullable": False},
            "updated_at": {"type": "timestamp", "nullable": True}
        },
        partition_columns=["year", "month", "day"],
        primary_key_columns=["customer_id"],
        read_options={
            "multiline": True,
            "inferSchema": True
        },
        validation_rules={
            "email_format": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "customer_id_length": {"min": 5, "max": 50}
        },
        environment="dev",
        is_active=True,
        tags={
            "data-source": "external",
            "pii": "true"
        }
    )
    
    # Create destination table configuration
    destination_table = TableConfigUpsert(
        table_id="customer-data-warehouse",
        pipeline_id=pipeline_id,
        table_name="dim_customer",
        table_type=TableType.DESTINATION,
        data_format=DataFormat.PARQUET,
        location="s3://data-warehouse-bucket/dimensions/customer/",
        schema_definition={
            "customer_key": {"type": "bigint", "nullable": False},
            "customer_id": {"type": "string", "nullable": False},
            "email_hash": {"type": "string", "nullable": False},
            "created_date": {"type": "date", "nullable": False},
            "is_active": {"type": "boolean", "nullable": False, "default": True}
        },
        primary_key_columns=["customer_key"],
        write_options={
            "mode": "overwrite",
            "partitionOverwriteMode": "dynamic"
        },
        retention_days=2555,  # 7 years
        environment="dev",
        is_active=True,
        tags={
            "data-type": "dimension",
            "security-level": "confidential"
        }
    )
    
    print(f"Source table: {source_table.table_id}")
    print(f"  - Type: {source_table.table_type}")
    print(f"  - Format: {source_table.data_format}")
    print(f"  - Location: {source_table.location}")
    
    print(f"\nDestination table: {destination_table.table_id}")
    print(f"  - Type: {destination_table.table_type}")
    print(f"  - Format: {destination_table.data_format}")
    print(f"  - Location: {destination_table.location}")
    
    try:
        # Note: In real usage, these would be created in DynamoDB
        print("✓ Table configurations would be created successfully")
        return [source_table.table_id, destination_table.table_id]
    except Exception as e:
        print(f"Demo note: {e} (Expected in demo environment)")
        return [source_table.table_id, destination_table.table_id]


def demonstrate_pipeline_run_logging(config, pipeline_id):
    """Demonstrate pipeline run logging with model-driven keys."""
    print("\n=== Pipeline Run Logging ===")
    
    # Initialize run logs APIs
    write_api = PipelineRunLogsWriteApi(config)
    read_api = PipelineRunLogsReadApi(config)
    
    # Create run log with model-driven key management
    run_data = PipelineRunLogUpsert(
        run_id="run-20241201-123456",
        pipeline_id=pipeline_id,  # Composite key: (run_id, pipeline_id)
        status=RunStatus.RUNNING,
        trigger_type="schedule",
        environment="dev",
        total_records_processed=0,
        input_tables=["customer-data-source"],
        output_tables=["customer-data-warehouse"],
        config_snapshot={
            "version": "v1.2.0",
            "cpu_cores": 4,
            "memory_gb": 16
        },
        tags={
            "execution-environment": "dev",
            "triggered-by": "airflow-scheduler"
        }
    )
    
    print(f"Pipeline run started:")
    print(f"  - Run ID: {run_data.run_id}")
    print(f"  - Pipeline ID: {run_data.pipeline_id}")
    print(f"  - Status: {run_data.status}")
    print(f"  - Trigger: {run_data.trigger_type}")
    print(f"  - Environment: {run_data.environment}")
    
    # Simulate processing stages
    processing_stages = [
        "data-validation",
        "data-transformation", 
        "data-quality-checks",
        "data-loading"
    ]
    
    print("\nProcessing stages:")
    for i, stage in enumerate(processing_stages, 1):
        print(f"  {i}. {stage}: ✓ (simulated)")
    
    # Update run status to success
    print(f"\n✓ Pipeline run {run_data.run_id} completed successfully")
    
    try:
        # Note: In real usage, this would update the run log in DynamoDB
        print("✓ Run log would be updated in DynamoDB")
        return run_data.run_id
    except Exception as e:
        print(f"Demo note: {e} (Expected in demo environment)")
        return run_data.run_id


def demonstrate_model_driven_features(config):
    """Demonstrate model-driven key management features."""
    print("\n=== Model-Driven Key Management ===")
    
    from dynamodb_wrapper.models.domain_models import PipelineConfig, PipelineRunLog
    from dynamodb_wrapper.utils import (
        get_model_primary_key_fields,
        get_model_gsi_names,
        build_model_key,
        build_gsi_key_condition
    )
    
    # Show model metadata
    print("1. Model Metadata:")
    print(f"   PipelineConfig keys: {get_model_primary_key_fields(PipelineConfig)}")
    print(f"   PipelineRunLog keys: {get_model_primary_key_fields(PipelineRunLog)}")
    print(f"   PipelineConfig GSIs: {get_model_gsi_names(PipelineConfig)}")
    print(f"   PipelineRunLog GSIs: {get_model_gsi_names(PipelineRunLog)}")
    
    # Show key building
    print("\n2. Key Building:")
    pipeline_key = build_model_key(PipelineConfig, pipeline_id="demo-pipeline")
    run_log_key = build_model_key(PipelineRunLog, run_id="run-123", pipeline_id="demo-pipeline")
    
    print(f"   Pipeline key: {pipeline_key}")
    print(f"   Run log key: {run_log_key}")
    
    # Show GSI conditions
    print("\n3. GSI Conditions:")
    try:
        active_condition = build_gsi_key_condition(
            PipelineConfig, 
            "ActivePipelinesIndex", 
            is_active=True
        )
        print("   ✓ ActivePipelinesIndex condition created")
        
        status_condition = build_gsi_key_condition(
            PipelineRunLog,
            "StatusRunsIndex",
            status="running"
        )
        print("   ✓ StatusRunsIndex condition created")
    except Exception as e:
        print(f"   Note: {e}")


def main():
    """Run the main demonstration."""
    print("🚀 DynamoDB Wrapper V2 - CQRS Architecture Examples")
    print("=" * 60)
    
    try:
        # Setup
        config = setup_configuration()
        
        # Demonstrate core features
        pipeline_id = demonstrate_pipeline_configuration(config)
        table_ids = demonstrate_table_configuration(config, pipeline_id)
        run_id = demonstrate_pipeline_run_logging(config, pipeline_id)
        demonstrate_model_driven_features(config)
        
        print("\n" + "=" * 60)
        print("✅ All examples completed successfully!")
        print("\n📝 Key Features Demonstrated:")
        print("• CQRS architecture with separate Read/Write APIs")
        print("• Model-driven key management")
        print("• Comprehensive configuration management")
        print("• Pipeline run logging with composite keys")
        print("• Type-safe domain models")
        print("• GSI-aware query building")
        
        print(f"\n📊 Created Resources (demo):")
        print(f"• Pipeline: {pipeline_id}")
        print(f"• Tables: {', '.join(table_ids)}")
        print(f"• Run: {run_id}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 This is a demonstration - some operations may not work without DynamoDB setup")


if __name__ == "__main__":
    main()