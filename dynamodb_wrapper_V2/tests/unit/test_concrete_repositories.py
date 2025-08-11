from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

from dynamodb_wrapper_V2.dynamodb_wrapper.config import DynamoDBConfig
from dynamodb_wrapper_V2.dynamodb_wrapper.models import (
    PipelineConfig,
    PipelineRunLog,
    RunStatus,
    TableConfig,
    TableType,
)
from dynamodb_wrapper_V2.dynamodb_wrapper.repositories import (
    PipelineConfigRepository,
    PipelineRunLogsRepository,
    TableConfigRepository,
)


class TestPipelineConfigRepository:
    """Test cases for PipelineConfigRepository."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DynamoDBConfig.for_local_development()

    @pytest.fixture
    def repository(self, config):
        """Create repository instance."""
        return PipelineConfigRepository(config)

    @pytest.fixture
    def sample_pipeline_config(self):
        """Create sample pipeline configuration."""
        return PipelineConfig(
            pipeline_id='test-pipeline',
            pipeline_name='Test Pipeline',
            source_type='s3',
            destination_type='redshift',
            description='Test pipeline for unit tests',
            is_active=True,
            environment='dev',
            created_by='test_user'
        )

    def test_repository_initialization(self, repository, config):
        """Test repository initialization."""
        assert repository.config == config
        assert repository.table_name == 'dev_pipeline_config'  # Updated to include environment prefix
        assert repository.model_class == PipelineConfig
        assert repository.primary_key == 'pipeline_id'

    def test_create_pipeline_config_method_structure(self, repository):
        """Test create_pipeline_config method signature and basic structure."""
        # Mock the create method to avoid DynamoDB calls
        with patch.object(repository, 'create') as mock_create:
            mock_pipeline = Mock(spec=PipelineConfig)
            mock_create.return_value = mock_pipeline

            repository.create_pipeline_config(
                pipeline_id='new-pipeline',
                pipeline_name='New Test Pipeline',
                source_type='s3',
                destination_type='bigquery',
                created_by='test_user'
            )

            # Verify create was called once
            mock_create.assert_called_once()

            # Verify the created pipeline has correct attributes
            created_pipeline_args = mock_create.call_args[0][0]
            assert created_pipeline_args.pipeline_id == 'new-pipeline'
            assert created_pipeline_args.pipeline_name == 'New Test Pipeline'
            assert created_pipeline_args.source_type == 's3'
            assert created_pipeline_args.destination_type == 'bigquery'
            assert created_pipeline_args.created_by == 'test_user'
            assert created_pipeline_args.is_active is True  # Default value

    def test_create_pipeline_config_with_timezone_context(self, repository):
        """Test create_pipeline_config with timezone parameter."""
        with patch.object(repository, 'create_with_timezone_context') as mock_create_tz:
            mock_pipeline = Mock(spec=PipelineConfig)
            mock_create_tz.return_value = mock_pipeline

            repository.create_pipeline_config(
                pipeline_id='tz-pipeline',
                pipeline_name='Timezone Test Pipeline',
                source_type='s3',
                destination_type='redshift',
                created_by='test_user',
                current_timezone='America/New_York'
            )

            # Verify timezone context method was called
            mock_create_tz.assert_called_once()
            args, kwargs = mock_create_tz.call_args
            assert len(args) == 2  # pipeline_config and timezone
            assert args[1] == 'America/New_York'

    def test_get_by_pipeline_id_basic(self, repository):
        """Test get_by_pipeline_id without timezone."""
        with patch.object(repository, 'get') as mock_get:
            mock_pipeline = Mock(spec=PipelineConfig)
            mock_get.return_value = mock_pipeline

            result = repository.get_by_pipeline_id('test-pipeline')

            mock_get.assert_called_once_with('test-pipeline')
            assert result == mock_pipeline

    def test_get_by_pipeline_id_with_timezone(self, repository):
        """Test get_by_pipeline_id with timezone parameter."""
        with patch.object(repository, 'get_with_timezone') as mock_get_tz:
            mock_pipeline = Mock(spec=PipelineConfig)
            mock_get_tz.return_value = mock_pipeline

            result = repository.get_by_pipeline_id(
                'test-pipeline',
                user_timezone='Europe/London'
            )

            mock_get_tz.assert_called_once_with(
                'test-pipeline',
                user_timezone='Europe/London'
            )
            assert result == mock_pipeline

    def test_get_active_pipelines_basic(self, repository):
        """Test get_active_pipelines without timezone."""
        with patch.object(repository, 'list_all') as mock_list:
            # Mock pipelines with different active states
            active_pipeline = Mock(spec=PipelineConfig)
            active_pipeline.is_active = True
            inactive_pipeline = Mock(spec=PipelineConfig)
            inactive_pipeline.is_active = False

            mock_list.return_value = [active_pipeline, inactive_pipeline]

            result = repository.get_active_pipelines()

            mock_list.assert_called_once()
            assert len(result) == 1
            assert result[0] == active_pipeline

    def test_get_active_pipelines_with_timezone(self, repository):
        """Test get_active_pipelines with timezone parameter."""
        with patch.object(repository, 'list_all_with_timezone') as mock_list_tz:
            active_pipeline = Mock(spec=PipelineConfig)
            active_pipeline.is_active = True

            mock_list_tz.return_value = [active_pipeline]

            result = repository.get_active_pipelines(user_timezone='Asia/Tokyo')

            mock_list_tz.assert_called_once_with('Asia/Tokyo')
            assert len(result) == 1
            assert result[0] == active_pipeline

    def test_get_pipelines_by_environment(self, repository):
        """Test get_pipelines_by_environment method."""
        with patch.object(repository, 'list_all') as mock_list:
            dev_pipeline = Mock(spec=PipelineConfig)
            dev_pipeline.environment = 'dev'
            prod_pipeline = Mock(spec=PipelineConfig)
            prod_pipeline.environment = 'prod'

            mock_list.return_value = [dev_pipeline, prod_pipeline]

            result = repository.get_pipelines_by_environment('dev')

            mock_list.assert_called_once()
            assert len(result) == 1
            assert result[0] == dev_pipeline

    def test_get_pipelines_by_environment_with_timezone(self, repository):
        """Test get_pipelines_by_environment with timezone parameter."""
        with patch.object(repository, 'list_all_with_timezone') as mock_list_tz:
            dev_pipeline = Mock(spec=PipelineConfig)
            dev_pipeline.environment = 'dev'

            mock_list_tz.return_value = [dev_pipeline]

            result = repository.get_pipelines_by_environment(
                'dev',
                user_timezone='Australia/Sydney'
            )

            mock_list_tz.assert_called_once_with('Australia/Sydney')
            assert len(result) == 1
            assert result[0] == dev_pipeline

    def test_update_pipeline_status_basic(self, repository):
        """Test update_pipeline_status without timezone."""
        with patch.object(repository, 'get_or_raise') as mock_get, \
             patch.object(repository, 'update') as mock_update:

            mock_pipeline = Mock(spec=PipelineConfig)
            mock_get.return_value = mock_pipeline
            mock_update.return_value = mock_pipeline

            repository.update_pipeline_status(
                'test-pipeline',
                is_active=False,
                updated_by='admin'
            )

            mock_get.assert_called_once_with('test-pipeline')
            assert mock_pipeline.is_active is False
            assert mock_pipeline.updated_by == 'admin'
            assert mock_pipeline.updated_at is not None
            mock_update.assert_called_once_with(mock_pipeline)

    def test_update_pipeline_status_with_timezone(self, repository):
        """Test update_pipeline_status with timezone parameter."""
        with patch.object(repository, 'get_or_raise') as mock_get, \
             patch.object(repository, 'update') as mock_update, \
             patch('dynamodb_wrapper_V2.dynamodb_wrapper.utils.timezone.now_in_tz') as mock_now_tz:

            mock_pipeline = Mock(spec=PipelineConfig)
            mock_get.return_value = mock_pipeline
            mock_update.return_value = mock_pipeline
            mock_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
            mock_now_tz.return_value = mock_time

            repository.update_pipeline_status(
                'test-pipeline',
                is_active=False,
                current_timezone='Pacific/Auckland'
            )

            mock_now_tz.assert_called_once_with('Pacific/Auckland')
            assert mock_pipeline.updated_at == mock_time


class TestTableConfigRepository:
    """Test cases for TableConfigRepository."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DynamoDBConfig.for_local_development()

    @pytest.fixture
    def repository(self, config):
        """Create repository instance."""
        return TableConfigRepository(config)

    def test_repository_initialization(self, repository, config):
        """Test repository initialization."""
        assert repository.config == config
        assert repository.table_name == 'dev_table_config'  # Updated to include environment prefix
        assert repository.model_class == TableConfig
        assert repository.primary_key == 'table_id'

    def test_create_table_config_method_structure(self, repository):
        """Test create_table_config method signature and basic structure."""
        with patch.object(repository, 'create') as mock_create:
            mock_table = Mock(spec=TableConfig)
            mock_create.return_value = mock_table

            repository.create_table_config(
                table_id='new-table',
                pipeline_id='test-pipeline',
                table_name='new_data_table',
                table_type=TableType.DESTINATION,
                data_format='json',
                location='s3://test-bucket/output/',
                created_by='test_user'
            )

            mock_create.assert_called_once()
            created_table_args = mock_create.call_args[0][0]
            assert created_table_args.table_id == 'new-table'
            assert created_table_args.pipeline_id == 'test-pipeline'
            assert created_table_args.table_type == TableType.DESTINATION
            assert created_table_args.data_format == 'json'

    def test_get_tables_by_pipeline(self, repository):
        """Test get_tables_by_pipeline method."""
        with patch.object(repository, 'list_all') as mock_list:
            table1 = Mock(spec=TableConfig)
            table1.pipeline_id = 'pipeline-1'
            table2 = Mock(spec=TableConfig)
            table2.pipeline_id = 'pipeline-1'
            table3 = Mock(spec=TableConfig)
            table3.pipeline_id = 'pipeline-2'

            mock_list.return_value = [table1, table2, table3]

            result = repository.get_tables_by_pipeline('pipeline-1')

            mock_list.assert_called_once()
            assert len(result) == 2
            assert table1 in result
            assert table2 in result
            assert table3 not in result

    def test_get_tables_by_type(self, repository):
        """Test get_tables_by_type method."""
        with patch.object(repository, 'list_all') as mock_list:
            source_table = Mock(spec=TableConfig)
            source_table.table_type = TableType.SOURCE
            source_table.pipeline_id = 'test-pipeline'

            dest_table = Mock(spec=TableConfig)
            dest_table.table_type = TableType.DESTINATION
            dest_table.pipeline_id = 'test-pipeline'

            other_source = Mock(spec=TableConfig)
            other_source.table_type = TableType.SOURCE
            other_source.pipeline_id = 'other-pipeline'

            mock_list.return_value = [source_table, dest_table, other_source]

            # Test getting all source tables
            result_all = repository.get_tables_by_type(TableType.SOURCE)
            assert len(result_all) == 2
            assert source_table in result_all
            assert other_source in result_all

            # Test getting source tables for specific pipeline
            result_filtered = repository.get_tables_by_type(
                TableType.SOURCE,
                pipeline_id='test-pipeline'
            )
            assert len(result_filtered) == 1
            assert result_filtered[0] == source_table

    def test_get_source_tables(self, repository):
        """Test get_source_tables convenience method."""
        with patch.object(repository, 'get_tables_by_type') as mock_get_type:
            mock_tables = [Mock(spec=TableConfig)]
            mock_get_type.return_value = mock_tables

            result = repository.get_source_tables('test-pipeline')

            mock_get_type.assert_called_once_with(
                TableType.SOURCE,
                'test-pipeline',
                None  # user_timezone
            )
            assert result == mock_tables

    def test_get_destination_tables(self, repository):
        """Test get_destination_tables convenience method."""
        with patch.object(repository, 'get_tables_by_type') as mock_get_type:
            mock_tables = [Mock(spec=TableConfig)]
            mock_get_type.return_value = mock_tables

            result = repository.get_destination_tables('test-pipeline')

            mock_get_type.assert_called_once_with(
                TableType.DESTINATION,
                'test-pipeline',
                None  # user_timezone
            )
            assert result == mock_tables

    def test_update_table_statistics(self, repository):
        """Test update_table_statistics method."""
        with patch.object(repository, 'get_or_raise') as mock_get, \
             patch.object(repository, 'update') as mock_update:

            mock_table = Mock(spec=TableConfig)
            mock_get.return_value = mock_table
            mock_update.return_value = mock_table

            last_updated = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

            repository.update_table_statistics(
                'test-table',
                record_count=1000,
                size_bytes=1024000,
                last_updated_data=last_updated
            )

            mock_get.assert_called_once_with('test-table')
            assert mock_table.record_count == 1000
            assert mock_table.size_bytes == 1024000
            assert mock_table.last_updated_data == last_updated
            assert mock_table.updated_at is not None
            mock_update.assert_called_once_with(mock_table)


class TestPipelineRunLogsRepository:
    """Test cases for PipelineRunLogsRepository."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DynamoDBConfig.for_local_development()

    @pytest.fixture
    def repository(self, config):
        """Create repository instance."""
        return PipelineRunLogsRepository(config)

    def test_repository_initialization(self, repository, config):
        """Test repository initialization."""
        assert repository.config == config
        assert repository.table_name == 'dev_pipeline_run_logs'  # Updated to include environment prefix
        assert repository.model_class == PipelineRunLog
        assert repository.primary_key == 'run_id'

    def test_create_run_log_method_structure(self, repository):
        """Test create_run_log method signature and basic structure."""
        with patch.object(repository, 'create') as mock_create:
            mock_run = Mock(spec=PipelineRunLog)
            mock_create.return_value = mock_run

            repository.create_run_log(
                run_id='new-run-001',
                pipeline_id='test-pipeline',
                trigger_type='manual',
                created_by='test_user'
            )

            mock_create.assert_called_once()
            created_run_args = mock_create.call_args[0][0]
            assert created_run_args.run_id == 'new-run-001'
            assert created_run_args.pipeline_id == 'test-pipeline'
            assert created_run_args.trigger_type == 'manual'
            assert created_run_args.status == RunStatus.PENDING
            assert created_run_args.created_by == 'test_user'

    def test_get_runs_by_pipeline(self, repository):
        """Test get_runs_by_pipeline method."""
        with patch.object(repository, 'list_all') as mock_list:
            # Create runs with different times for sorting test
            run1 = Mock(spec=PipelineRunLog)
            run1.pipeline_id = 'test-pipeline'
            run1.start_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

            run2 = Mock(spec=PipelineRunLog)
            run2.pipeline_id = 'test-pipeline'
            run2.start_time = datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)

            run3 = Mock(spec=PipelineRunLog)
            run3.pipeline_id = 'other-pipeline'
            run3.start_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

            mock_list.return_value = [run1, run2, run3]

            result = repository.get_runs_by_pipeline('test-pipeline')

            mock_list.assert_called_once()
            assert len(result) == 2
            # Should be sorted by start_time descending (most recent first)
            assert result[0] == run2  # 11:00
            assert result[1] == run1  # 10:00

    def test_get_runs_by_pipeline_with_limit(self, repository):
        """Test get_runs_by_pipeline with limit parameter."""
        with patch.object(repository, 'list_all') as mock_list:
            # Create more runs than the limit
            runs = []
            for i in range(5):
                run = Mock(spec=PipelineRunLog)
                run.pipeline_id = 'test-pipeline'
                run.start_time = datetime(2024, 1, 15, 10 + i, 0, 0, tzinfo=timezone.utc)
                runs.append(run)

            mock_list.return_value = runs

            result = repository.get_runs_by_pipeline('test-pipeline', limit=3)

            assert len(result) == 3
            # Should be the 3 most recent runs
            assert result[0].start_time.hour == 14  # Most recent
            assert result[1].start_time.hour == 13
            assert result[2].start_time.hour == 12

    def test_get_runs_by_status(self, repository):
        """Test get_runs_by_status method."""
        with patch.object(repository, 'list_all') as mock_list:
            success_run1 = Mock(spec=PipelineRunLog)
            success_run1.status = RunStatus.SUCCESS
            success_run1.pipeline_id = 'test-pipeline'

            success_run2 = Mock(spec=PipelineRunLog)
            success_run2.status = RunStatus.SUCCESS
            success_run2.pipeline_id = 'other-pipeline'

            failed_run = Mock(spec=PipelineRunLog)
            failed_run.status = RunStatus.FAILED
            failed_run.pipeline_id = 'test-pipeline'

            mock_list.return_value = [success_run1, success_run2, failed_run]

            # Get all successful runs
            success_results = repository.get_runs_by_status(RunStatus.SUCCESS)
            assert len(success_results) == 2
            assert success_run1 in success_results
            assert success_run2 in success_results

            # Get successful runs for specific pipeline
            pipeline_success = repository.get_runs_by_status(
                RunStatus.SUCCESS,
                pipeline_id='test-pipeline'
            )
            assert len(pipeline_success) == 1
            assert pipeline_success[0] == success_run1

    def test_get_running_pipelines(self, repository):
        """Test get_running_pipelines method."""
        with patch.object(repository, 'get_runs_by_status') as mock_get_status:
            mock_runs = [Mock(spec=PipelineRunLog)]
            mock_get_status.return_value = mock_runs

            result = repository.get_running_pipelines()

            mock_get_status.assert_called_once_with(
                RunStatus.RUNNING,
                user_timezone=None
            )
            assert result == mock_runs

    def test_update_run_status(self, repository):
        """Test update_run_status method."""
        with patch.object(repository, 'get_or_raise') as mock_get, \
             patch.object(repository, 'update') as mock_update:

            mock_run = Mock(spec=PipelineRunLog)
            mock_run.start_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
            mock_get.return_value = mock_run
            mock_update.return_value = mock_run

            end_time = datetime(2024, 1, 15, 12, 30, 0, tzinfo=timezone.utc)

            repository.update_run_status(
                'test-run-001',
                RunStatus.SUCCESS,
                end_time=end_time
            )

            mock_get.assert_called_once_with('test-run-001')
            assert mock_run.status == RunStatus.SUCCESS
            assert mock_run.end_time == end_time
            # Duration should be calculated (2.5 hours = 9000 seconds)
            assert mock_run.duration_seconds == 9000.0
            assert mock_run.updated_at is not None
            mock_update.assert_called_once_with(mock_run)

    def test_update_run_status_auto_end_time(self, repository):
        """Test update_run_status auto-sets end_time for finished runs."""
        with patch.object(repository, 'get_or_raise') as mock_get, \
             patch.object(repository, 'update') as mock_update:

            mock_run = Mock(spec=PipelineRunLog)
            mock_run.start_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
            mock_get.return_value = mock_run
            mock_update.return_value = mock_run

            # Mock datetime.now() to return timezone-aware datetime
            with patch('dynamodb_wrapper_V2.dynamodb_wrapper.repositories.pipeline_run_logs.datetime') as mock_datetime:
                mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
                mock_datetime.now.return_value = mock_now
                mock_datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

                repository.update_run_status(
                    'test-run-001',
                    RunStatus.SUCCESS  # No explicit end_time
                )

            assert mock_run.status == RunStatus.SUCCESS
            assert mock_run.end_time == mock_now  # Should be auto-set
            assert mock_run.duration_seconds == 7200.0  # 2 hours

    def test_add_stage_info(self, repository):
        """Test add_stage_info method."""
        with patch.object(repository, 'get_or_raise') as mock_get, \
             patch.object(repository, 'update') as mock_update:

            mock_run = Mock(spec=PipelineRunLog)
            mock_run.stages = []
            mock_get.return_value = mock_run
            mock_update.return_value = mock_run

            stage_info = {
                'stage_name': 'data_extraction',
                'status': 'completed',
                'records_processed': 1000
            }

            repository.add_stage_info('test-run-001', stage_info)

            mock_get.assert_called_once_with('test-run-001')
            assert len(mock_run.stages) == 1
            assert mock_run.stages[0] == stage_info
            assert mock_run.updated_at is not None
            mock_update.assert_called_once_with(mock_run)

    def test_update_existing_stage_info(self, repository):
        """Test updating existing stage info."""
        with patch.object(repository, 'get_or_raise') as mock_get, \
             patch.object(repository, 'update') as mock_update:

            existing_stage = {
                'stage_name': 'data_processing',
                'status': 'running'
            }

            mock_run = Mock(spec=PipelineRunLog)
            mock_run.stages = [existing_stage]
            mock_get.return_value = mock_run
            mock_update.return_value = mock_run

            updated_stage_info = {
                'stage_name': 'data_processing',
                'status': 'completed',
                'records_processed': 5000
            }

            repository.add_stage_info('test-run-001', updated_stage_info)

            assert len(mock_run.stages) == 1
            assert mock_run.stages[0] == updated_stage_info
            assert mock_run.updated_at is not None
