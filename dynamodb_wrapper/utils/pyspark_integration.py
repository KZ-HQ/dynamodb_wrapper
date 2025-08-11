import logging
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType
    PYSPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    StructType = None
    PYSPARK_AVAILABLE = False

from ..config import DynamoDBConfig
from ..models import PipelineConfig, RunStatus, TableConfig
from ..repositories import (
    PipelineConfigRepository,
    PipelineRunLogsRepository,
    TableConfigRepository,
)

logger = logging.getLogger(__name__)


class SparkDynamoDBIntegration:
    """Integration utilities for using DynamoDB wrapper in PySpark environments."""

    def __init__(self, config: DynamoDBConfig):
        """Initialize with DynamoDB configuration.

        Args:
            config: DynamoDB configuration
        """
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is not available. Please install PySpark to use this functionality.")

        self.config = config
        self.pipeline_repo = PipelineConfigRepository(config)
        self.table_repo = TableConfigRepository(config)
        self.logs_repo = PipelineRunLogsRepository(config)

    def get_spark_config_from_pipeline(self, pipeline_id: str) -> Dict[str, str]:
        """Get Spark configuration from pipeline configuration.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Dictionary of Spark configuration parameters
        """
        pipeline_config = self.pipeline_repo.get_or_raise(pipeline_id)

        spark_config = {
            "spark.app.name": f"Pipeline-{pipeline_config.pipeline_name}",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        }

        # Add pipeline-specific Spark config
        if pipeline_config.spark_config:
            spark_config.update(pipeline_config.spark_config)

        # Add resource requirements if specified
        if pipeline_config.cpu_cores:
            spark_config["spark.executor.cores"] = str(pipeline_config.cpu_cores)

        if pipeline_config.memory_gb:
            memory_mb = int(pipeline_config.memory_gb * 1024)
            spark_config["spark.executor.memory"] = f"{memory_mb}m"

        # Add AWS configuration
        spark_config.update({
            "spark.hadoop.fs.s3a.access.key": self.config.aws_access_key_id or "",
            "spark.hadoop.fs.s3a.secret.key": self.config.aws_secret_access_key or "",
            "spark.hadoop.fs.s3a.endpoint.region": self.config.region_name,
        })

        return spark_config

    def create_spark_session(self, pipeline_id: str, additional_config: Dict[str, str] = None) -> SparkSession:
        """Create Spark session with configuration from pipeline.

        Args:
            pipeline_id: Pipeline identifier
            additional_config: Additional Spark configuration to merge

        Returns:
            Configured SparkSession
        """
        spark_config = self.get_spark_config_from_pipeline(pipeline_id)

        if additional_config:
            spark_config.update(additional_config)

        builder = SparkSession.builder

        for key, value in spark_config.items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()

        logger.info(f"Created Spark session for pipeline {pipeline_id}")
        return spark

    def get_table_read_options(self, table_id: str) -> Dict[str, Any]:
        """Get read options for a table from its configuration.

        Args:
            table_id: Table identifier

        Returns:
            Dictionary of read options for Spark
        """
        table_config = self.table_repo.get_or_raise(table_id)

        read_options = {
            "format": table_config.data_format.value,
            "path": table_config.location,
        }

        # Add format-specific options
        if table_config.read_options:
            read_options.update(table_config.read_options)

        # Add partition information if available
        if table_config.partition_columns:
            read_options["partitionColumns"] = table_config.partition_columns

        return read_options

    def get_table_write_options(self, table_id: str) -> Dict[str, Any]:
        """Get write options for a table from its configuration.

        Args:
            table_id: Table identifier

        Returns:
            Dictionary of write options for Spark
        """
        table_config = self.table_repo.get_or_raise(table_id)

        write_options = {
            "format": table_config.data_format.value,
            "path": table_config.location,
            "mode": "overwrite",  # Default mode
        }

        # Add format-specific options
        if table_config.write_options:
            write_options.update(table_config.write_options)

        # Add partition information if available
        if table_config.partition_columns:
            write_options["partitionBy"] = table_config.partition_columns

        return write_options

    @contextmanager
    def pipeline_run_context(self, pipeline_id: str, trigger_type: str = "manual", created_by: str = None):
        """Context manager for pipeline runs with automatic logging.

        Args:
            pipeline_id: Pipeline identifier
            trigger_type: What triggered the run
            created_by: User who triggered the run
        """
        run_id = str(uuid.uuid4())

        # Create initial run log
        self.logs_repo.create_run_log(
            run_id=run_id,
            pipeline_id=pipeline_id,
            trigger_type=trigger_type,
            created_by=created_by
        )

        try:
            # Update status to running
            self.logs_repo.update_run_status(run_id, RunStatus.RUNNING)
            logger.info(f"Started pipeline run {run_id} for pipeline {pipeline_id}")

            yield run_id

            # If we get here, the pipeline completed successfully
            self.logs_repo.update_run_status(run_id, RunStatus.SUCCESS)
            logger.info(f"Pipeline run {run_id} completed successfully")

        except Exception as e:
            # Log the failure
            error_message = str(e)
            self.logs_repo.update_run_status(
                run_id,
                RunStatus.FAILED,
                error_message=error_message
            )
            logger.error(f"Pipeline run {run_id} failed: {error_message}")
            raise

    def update_table_stats_after_write(self, table_id: str, df, run_id: str = None):
        """Update table statistics after writing data.

        Args:
            table_id: Table identifier
            df: Spark DataFrame that was written
            run_id: Optional run ID to update in logs
        """
        try:
            record_count = df.count()

            # Update table configuration with new stats
            self.table_repo.update_table_statistics(
                table_id=table_id,
                record_count=record_count,
                last_updated_data=datetime.now(timezone.utc)
            )

            # Update run log if provided
            if run_id:
                run_log = self.logs_repo.get(run_id)
                if run_log:
                    if not run_log.output_tables:
                        run_log.output_tables = []
                    if table_id not in run_log.output_tables:
                        run_log.output_tables.append(table_id)

                    run_log.total_records_processed = (run_log.total_records_processed or 0) + record_count
                    self.logs_repo.update(run_log)

            logger.info(f"Updated stats for table {table_id}: {record_count} records")

        except Exception as e:
            logger.warning(f"Failed to update table stats for {table_id}: {e}")


# Standalone utility functions for use without the class
def get_pipeline_config_for_spark(pipeline_id: str, config: DynamoDBConfig = None) -> PipelineConfig:
    """Get pipeline configuration for Spark usage.

    Args:
        pipeline_id: Pipeline identifier
        config: Optional DynamoDB config, uses environment if not provided

    Returns:
        PipelineConfig instance
    """
    if config is None:
        config = DynamoDBConfig.for_pyspark()

    repo = PipelineConfigRepository(config)
    return repo.get_or_raise(pipeline_id)


def get_table_configs_for_spark(pipeline_id: str, config: DynamoDBConfig = None) -> List[TableConfig]:
    """Get table configurations for a pipeline for Spark usage.

    Args:
        pipeline_id: Pipeline identifier
        config: Optional DynamoDB config, uses environment if not provided

    Returns:
        List of TableConfig instances
    """
    if config is None:
        config = DynamoDBConfig.for_pyspark()

    repo = TableConfigRepository(config)
    return repo.get_active_tables_by_pipeline(pipeline_id)


def log_pipeline_run_from_spark(
    pipeline_id: str,
    trigger_type: str = "spark",
    config: DynamoDBConfig = None,
    **kwargs
) -> str:
    """Create a pipeline run log from Spark.

    Args:
        pipeline_id: Pipeline identifier
        trigger_type: What triggered the run
        config: Optional DynamoDB config, uses environment if not provided
        **kwargs: Additional run log parameters

    Returns:
        Run ID of the created log
    """
    if config is None:
        config = DynamoDBConfig.for_pyspark()

    run_id = str(uuid.uuid4())
    repo = PipelineRunLogsRepository(config)

    repo.create_run_log(
        run_id=run_id,
        pipeline_id=pipeline_id,
        trigger_type=trigger_type,
        **kwargs
    )

    return run_id


def create_spark_session_with_dynamodb(
    app_name: str,
    pipeline_id: str = None,
    config: DynamoDBConfig = None,
    additional_config: Dict[str, str] = None
) -> SparkSession:
    """Create Spark session with DynamoDB integration.

    Args:
        app_name: Spark application name
        pipeline_id: Optional pipeline ID to load configuration from
        config: Optional DynamoDB config
        additional_config: Additional Spark configuration

    Returns:
        Configured SparkSession
    """
    if config is None:
        config = DynamoDBConfig.for_pyspark()

    if pipeline_id:
        integration = SparkDynamoDBIntegration(config)
        return integration.create_spark_session(pipeline_id, additional_config)
    else:
        # Create basic session with AWS configuration
        spark_config = {
            "spark.app.name": app_name,
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.hadoop.fs.s3a.access.key": config.aws_access_key_id or "",
            "spark.hadoop.fs.s3a.secret.key": config.aws_secret_access_key or "",
            "spark.hadoop.fs.s3a.endpoint.region": config.region_name,
        }

        if additional_config:
            spark_config.update(additional_config)

        builder = SparkSession.builder

        for key, value in spark_config.items():
            builder = builder.config(key, value)

        return builder.getOrCreate()
