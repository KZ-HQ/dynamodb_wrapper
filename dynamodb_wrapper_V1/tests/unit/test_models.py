from datetime import datetime

from dynamodb_wrapper_V1.dynamodb_wrapper.models import PipelineConfig, PipelineRunLog, TableConfig
from dynamodb_wrapper_V1.dynamodb_wrapper.models.pipeline_run_log import LogLevel, RunStatus
from dynamodb_wrapper_V1.dynamodb_wrapper.models.table_config import DataFormat, TableType


class TestPipelineConfig:
    """Test cases for PipelineConfig model."""

    def test_pipeline_config_creation(self):
        """Test creating a pipeline configuration."""
        config = PipelineConfig(
            pipeline_id="test-pipeline",
            pipeline_name="Test Pipeline",
            source_type="s3",
            destination_type="redshift"
        )

        assert config.pipeline_id == "test-pipeline"
        assert config.pipeline_name == "Test Pipeline"
        assert config.source_type == "s3"
        assert config.destination_type == "redshift"
        assert config.is_active is True
        assert config.environment == "dev"
        assert config.version == "1.0.0"
        assert isinstance(config.created_at, datetime)
        assert isinstance(config.updated_at, datetime)

    def test_pipeline_config_with_optional_fields(self):
        """Test pipeline configuration with optional fields."""
        spark_config = {"spark.sql.adaptive.enabled": "true"}
        tags = {"team": "data", "project": "analytics"}

        config = PipelineConfig(
            pipeline_id="test-pipeline",
            pipeline_name="Test Pipeline",
            description="A test pipeline",
            source_type="s3",
            destination_type="redshift",
            schedule_expression="0 9 * * *",
            spark_config=spark_config,
            cpu_cores=4,
            memory_gb=8.0,
            tags=tags,
            created_by="test_user"
        )

        assert config.description == "A test pipeline"
        assert config.schedule_expression == "0 9 * * *"
        assert config.spark_config == spark_config
        assert config.cpu_cores == 4
        assert config.memory_gb == 8.0
        assert config.tags == tags
        assert config.created_by == "test_user"

    def test_pipeline_config_json_serialization(self):
        """Test JSON serialization of pipeline configuration."""
        config = PipelineConfig(
            pipeline_id="test-pipeline",
            pipeline_name="Test Pipeline",
            source_type="s3",
            destination_type="redshift"
        )

        json_data = config.model_dump()

        assert json_data["pipeline_id"] == "test-pipeline"
        assert json_data["pipeline_name"] == "Test Pipeline"
        assert "created_at" in json_data
        assert "updated_at" in json_data


class TestTableConfig:
    """Test cases for TableConfig model."""

    def test_table_config_creation(self):
        """Test creating a table configuration."""
        config = TableConfig(
            table_id="test-table",
            pipeline_id="test-pipeline",
            table_name="test_table",
            table_type=TableType.SOURCE,
            data_format=DataFormat.PARQUET,
            location="s3://bucket/path/"
        )

        assert config.table_id == "test-table"
        assert config.pipeline_id == "test-pipeline"
        assert config.table_name == "test_table"
        assert config.table_type == TableType.SOURCE
        assert config.data_format == DataFormat.PARQUET
        assert config.location == "s3://bucket/path/"
        assert config.is_active is True
        assert config.environment == "dev"
        assert config.caching_enabled is False

    def test_table_config_with_schema(self):
        """Test table configuration with schema definition."""
        schema_def = {
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "timestamp", "type": "timestamp"}
            ]
        }

        config = TableConfig(
            table_id="test-table",
            pipeline_id="test-pipeline",
            table_name="test_table",
            table_type=TableType.DESTINATION,
            data_format=DataFormat.JSON,
            location="s3://bucket/path/",
            schema_definition=schema_def,
            partition_columns=["date"],
            primary_key_columns=["id"]
        )

        assert config.schema_definition == schema_def
        assert config.partition_columns == ["date"]
        assert config.primary_key_columns == ["id"]

    def test_table_type_enum(self):
        """Test table type enumeration."""
        assert TableType.SOURCE == "source"
        assert TableType.DESTINATION == "destination"
        assert TableType.LOOKUP == "lookup"
        assert TableType.INTERMEDIATE == "intermediate"

    def test_data_format_enum(self):
        """Test data format enumeration."""
        assert DataFormat.PARQUET == "parquet"
        assert DataFormat.JSON == "json"
        assert DataFormat.CSV == "csv"
        assert DataFormat.AVRO == "avro"
        assert DataFormat.DELTA == "delta"


class TestPipelineRunLog:
    """Test cases for PipelineRunLog model."""

    def test_pipeline_run_log_creation(self):
        """Test creating a pipeline run log."""
        log = PipelineRunLog(
            run_id="test-run-123",
            pipeline_id="test-pipeline",
            status=RunStatus.PENDING,
            trigger_type="manual"
        )

        assert log.run_id == "test-run-123"
        assert log.pipeline_id == "test-pipeline"
        assert log.status == RunStatus.PENDING
        assert log.trigger_type == "manual"
        assert log.retry_count == 0
        assert log.data_quality_passed is True
        assert log.log_level == LogLevel.INFO
        assert isinstance(log.start_time, datetime)
        assert isinstance(log.created_at, datetime)

    def test_pipeline_run_log_with_stages(self):
        """Test pipeline run log with stage information."""
        from dynamodb_wrapper_V1.dynamodb_wrapper.models.pipeline_run_log import StageInfo

        stage = StageInfo(
            stage_name="extract",
            status=RunStatus.SUCCESS,
            start_time=datetime.utcnow(),
            records_processed=1000
        )

        log = PipelineRunLog(
            run_id="test-run-123",
            pipeline_id="test-pipeline",
            status=RunStatus.RUNNING,
            trigger_type="schedule",
            stages=[stage],
            current_stage="transform"
        )

        assert len(log.stages) == 1
        assert log.stages[0].stage_name == "extract"
        assert log.stages[0].status == RunStatus.SUCCESS
        assert log.stages[0].records_processed == 1000
        assert log.current_stage == "transform"

    def test_pipeline_run_log_with_data_quality(self):
        """Test pipeline run log with data quality results."""
        from dynamodb_wrapper_V1.dynamodb_wrapper.models.pipeline_run_log import DataQualityResult

        dq_result = DataQualityResult(
            check_name="row_count",
            passed=True,
            expected_value=1000,
            actual_value=1000
        )

        log = PipelineRunLog(
            run_id="test-run-123",
            pipeline_id="test-pipeline",
            status=RunStatus.SUCCESS,
            trigger_type="manual",
            data_quality_results=[dq_result],
            total_records_processed=1000
        )

        assert len(log.data_quality_results) == 1
        assert log.data_quality_results[0].check_name == "row_count"
        assert log.data_quality_results[0].passed is True
        assert log.total_records_processed == 1000

    def test_run_status_enum(self):
        """Test run status enumeration."""
        assert RunStatus.PENDING == "pending"
        assert RunStatus.RUNNING == "running"
        assert RunStatus.SUCCESS == "success"
        assert RunStatus.FAILED == "failed"
        assert RunStatus.CANCELLED == "cancelled"
        assert RunStatus.SKIPPED == "skipped"

    def test_log_level_enum(self):
        """Test log level enumeration."""
        assert LogLevel.DEBUG == "debug"
        assert LogLevel.INFO == "info"
        assert LogLevel.WARNING == "warning"
        assert LogLevel.ERROR == "error"
        assert LogLevel.CRITICAL == "critical"
