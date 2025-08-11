from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Optional

from ..config import DynamoDBConfig
from ..models import PipelineRunLog, RunStatus
from .base import BaseDynamoRepository


class PipelineRunLogsRepository(BaseDynamoRepository[PipelineRunLog]):
    """Repository for pipeline run log operations."""

    def __init__(self, config: DynamoDBConfig):
        """Initialize repository with DynamoDB configuration."""
        super().__init__(config)

    @property
    def table_name(self) -> str:
        """Return the DynamoDB table name."""
        return self.config.get_table_name("pipeline_run_logs")

    @property
    def model_class(self) -> type[PipelineRunLog]:
        """Return the Pydantic model class."""
        return PipelineRunLog

    @property
    def primary_key(self) -> str:
        """Return the primary key field name."""
        return "run_id"

    def get_by_run_id(self, run_id: str, user_timezone: Optional[str] = None) -> Optional[PipelineRunLog]:
        """Get pipeline run log by run ID.

        Args:
            run_id: The run identifier
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            PipelineRunLog if found, None otherwise
        """
        if user_timezone:
            return self.get_with_timezone(run_id, user_timezone=user_timezone)
        return self.get(run_id)

    def get_runs_by_pipeline(self, pipeline_id: str, limit: Optional[int] = None, user_timezone: Optional[str] = None) -> List[PipelineRunLog]:
        """Get pipeline run logs for a specific pipeline.

        Args:
            pipeline_id: The pipeline identifier
            limit: Maximum number of runs to return
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of PipelineRunLog instances, sorted by start_time descending
        """
        if user_timezone:
            all_runs = self.list_all_with_timezone(user_timezone)
        else:
            all_runs = self.list_all()
        pipeline_runs = [run for run in all_runs if run.pipeline_id == pipeline_id]

        # Sort by start_time descending (most recent first)
        pipeline_runs.sort(key=lambda x: x.start_time, reverse=True)

        if limit:
            pipeline_runs = pipeline_runs[:limit]

        return pipeline_runs

    def get_runs_by_status(self, status: RunStatus, pipeline_id: Optional[str] = None, user_timezone: Optional[str] = None) -> List[PipelineRunLog]:
        """Get pipeline runs by status.

        Args:
            status: The run status to filter by
            pipeline_id: Optional pipeline ID to further filter
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of PipelineRunLog instances with the specified status
        """
        if user_timezone:
            all_runs = self.list_all_with_timezone(user_timezone)
        else:
            all_runs = self.list_all()
        filtered_runs = [run for run in all_runs if run.status == status]

        if pipeline_id:
            filtered_runs = [run for run in filtered_runs if run.pipeline_id == pipeline_id]

        return filtered_runs

    def get_running_pipelines(self, user_timezone: Optional[str] = None) -> List[PipelineRunLog]:
        """Get all currently running pipeline runs.

        Args:
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of PipelineRunLog instances with RUNNING status
        """
        return self.get_runs_by_status(RunStatus.RUNNING, user_timezone=user_timezone)

    def get_failed_runs(self, pipeline_id: Optional[str] = None, hours: int = 24, user_timezone: Optional[str] = None) -> List[PipelineRunLog]:
        """Get failed pipeline runs within specified time window.

        Args:
            pipeline_id: Optional pipeline ID to filter
            hours: Number of hours to look back (default: 24)
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of failed PipelineRunLog instances
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        failed_runs = self.get_runs_by_status(RunStatus.FAILED, pipeline_id, user_timezone)

        return [run for run in failed_runs if run.start_time >= cutoff_time]

    def get_recent_runs(self, pipeline_id: str, hours: int = 24, user_timezone: Optional[str] = None) -> List[PipelineRunLog]:
        """Get recent pipeline runs within specified time window.

        Args:
            pipeline_id: The pipeline identifier
            hours: Number of hours to look back (default: 24)
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of recent PipelineRunLog instances
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        pipeline_runs = self.get_runs_by_pipeline(pipeline_id, user_timezone=user_timezone)

        return [run for run in pipeline_runs if run.start_time >= cutoff_time]

    def update_run_status(
        self,
        run_id: str,
        status: RunStatus,
        error_message: Optional[str] = None,
        end_time: Optional[datetime] = None
    ) -> PipelineRunLog:
        """Update pipeline run status.

        Args:
            run_id: The run identifier
            status: New status
            error_message: Error message if failed
            end_time: End time if completed

        Returns:
            Updated PipelineRunLog instance
        """
        run_log = self.get_or_raise(run_id)
        run_log.status = status
        run_log.updated_at = datetime.now(timezone.utc)

        if error_message:
            run_log.error_message = error_message

        if end_time:
            run_log.end_time = end_time
            if run_log.start_time:
                duration = (end_time - run_log.start_time).total_seconds()
                run_log.duration_seconds = Decimal(str(duration))
        elif status in [RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED]:
            # Auto-set end time if not provided and run is finished
            run_log.end_time = datetime.now(timezone.utc)
            if run_log.start_time:
                duration = (run_log.end_time - run_log.start_time).total_seconds()
                run_log.duration_seconds = Decimal(str(duration))

        return self.update(run_log)

    def create_run_log(
        self,
        run_id: str,
        pipeline_id: str,
        trigger_type: str,
        created_by: Optional[str] = None,
        **kwargs
    ) -> PipelineRunLog:
        """Create a new pipeline run log.

        Args:
            run_id: Unique run identifier
            pipeline_id: Pipeline this run belongs to
            trigger_type: What triggered the run
            created_by: User who triggered the run
            **kwargs: Additional run log parameters

        Returns:
            Created PipelineRunLog instance
        """
        log_data = {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "trigger_type": trigger_type,
            "status": RunStatus.PENDING,
            "created_by": created_by,
            **kwargs
        }

        run_log = PipelineRunLog(**log_data)
        return self.create(run_log)

    def add_stage_info(self, run_id: str, stage_info: dict) -> PipelineRunLog:
        """Add or update stage information for a pipeline run.

        Args:
            run_id: The run identifier
            stage_info: Stage information dictionary

        Returns:
            Updated PipelineRunLog instance
        """
        run_log = self.get_or_raise(run_id)

        # Update existing stage or add new one
        stage_name = stage_info.get("stage_name")
        existing_stage_index = None

        for i, stage in enumerate(run_log.stages):
            # Handle both dict and object formats
            stage_name_attr = getattr(stage, 'stage_name', None) or stage.get('stage_name')
            if stage_name_attr == stage_name:
                existing_stage_index = i
                break

        if existing_stage_index is not None:
            # Update existing stage
            run_log.stages[existing_stage_index] = stage_info
        else:
            # Add new stage
            run_log.stages.append(stage_info)

        run_log.updated_at = datetime.now(timezone.utc)
        return self.update(run_log)


