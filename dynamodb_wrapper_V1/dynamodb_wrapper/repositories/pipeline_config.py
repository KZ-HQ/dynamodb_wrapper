from datetime import datetime, timezone
from typing import List, Optional

from ..config import DynamoDBConfig
from ..models import PipelineConfig
from .base import BaseDynamoRepository


class PipelineConfigRepository(BaseDynamoRepository[PipelineConfig]):
    """Repository for pipeline configuration operations."""

    def __init__(self, config: DynamoDBConfig):
        """Initialize repository with DynamoDB configuration."""
        super().__init__(config)

    @property
    def table_name(self) -> str:
        """Return the DynamoDB table name."""
        return self.config.get_table_name("pipeline_config")

    @property
    def model_class(self) -> type[PipelineConfig]:
        """Return the Pydantic model class."""
        return PipelineConfig

    @property
    def primary_key(self) -> str:
        """Return the primary key field name."""
        return "pipeline_id"

    def get_by_pipeline_id(self, pipeline_id: str, user_timezone: Optional[str] = None) -> Optional[PipelineConfig]:
        """Get pipeline configuration by pipeline ID.

        Args:
            pipeline_id: The pipeline identifier
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            PipelineConfig if found, None otherwise
        """
        if user_timezone:
            return self.get_with_timezone(pipeline_id, user_timezone=user_timezone)
        return self.get(pipeline_id)

    def get_active_pipelines(self, user_timezone: Optional[str] = None) -> List[PipelineConfig]:
        """Get all active pipeline configurations.

        Args:
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of active PipelineConfig instances
        """
        if user_timezone:
            all_pipelines = self.list_all_with_timezone(user_timezone)
        else:
            all_pipelines = self.list_all()
        return [pipeline for pipeline in all_pipelines if pipeline.is_active]

    def get_pipelines_by_environment(self, environment: str, user_timezone: Optional[str] = None) -> List[PipelineConfig]:
        """Get pipeline configurations by environment.

        Args:
            environment: Environment to filter by (dev, staging, prod)
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of PipelineConfig instances for the specified environment
        """
        if user_timezone:
            all_pipelines = self.list_all_with_timezone(user_timezone)
        else:
            all_pipelines = self.list_all()
        return [pipeline for pipeline in all_pipelines if pipeline.environment == environment]

    def update_pipeline_status(self, pipeline_id: str, is_active: bool, updated_by: Optional[str] = None, current_timezone: Optional[str] = None) -> PipelineConfig:
        """Update pipeline active status.

        Args:
            pipeline_id: The pipeline identifier
            is_active: New active status
            updated_by: User making the update
            current_timezone: Optional timezone to use for updated_at timestamp

        Returns:
            Updated PipelineConfig instance
        """
        pipeline = self.get_or_raise(pipeline_id)
        pipeline.is_active = is_active

        # Use timezone-aware datetime if timezone specified
        if current_timezone:
            from ..utils.timezone import now_in_tz
            pipeline.updated_at = now_in_tz(current_timezone)
        else:
            pipeline.updated_at = datetime.now(timezone.utc)

        if updated_by:
            pipeline.updated_by = updated_by

        return self.update(pipeline)

    def create_pipeline_config(
        self,
        pipeline_id: str,
        pipeline_name: str,
        source_type: str,
        destination_type: str,
        created_by: Optional[str] = None,
        current_timezone: Optional[str] = None,
        **kwargs
    ) -> PipelineConfig:
        """Create a new pipeline configuration.

        Args:
            pipeline_id: Unique pipeline identifier
            pipeline_name: Human-readable pipeline name
            source_type: Type of data source
            destination_type: Type of destination
            created_by: User creating the pipeline
            current_timezone: Optional timezone to use for created_at timestamp
            **kwargs: Additional pipeline configuration parameters

        Returns:
            Created PipelineConfig instance
        """
        config_data = {
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_name,
            "source_type": source_type,
            "destination_type": destination_type,
            "created_by": created_by,
            **kwargs
        }

        pipeline_config = PipelineConfig(**config_data)

        # Use timezone context if specified
        if current_timezone:
            return self.create_with_timezone_context(pipeline_config, current_timezone)
        else:
            return self.create(pipeline_config)
