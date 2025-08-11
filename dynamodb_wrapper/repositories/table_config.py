from datetime import datetime, timezone
from typing import List, Optional

from ..config import DynamoDBConfig
from ..models import TableConfig, TableType
from .base import BaseDynamoRepository


class TableConfigRepository(BaseDynamoRepository[TableConfig]):
    """Repository for table configuration operations."""

    def __init__(self, config: DynamoDBConfig):
        """Initialize repository with DynamoDB configuration."""
        super().__init__(config)

    @property
    def table_name(self) -> str:
        """Return the DynamoDB table name."""
        return self.config.get_table_name("table_config")

    @property
    def model_class(self) -> type[TableConfig]:
        """Return the Pydantic model class."""
        return TableConfig

    @property
    def primary_key(self) -> str:
        """Return the primary key field name."""
        return "table_id"

    def get_by_table_id(self, table_id: str, user_timezone: Optional[str] = None) -> Optional[TableConfig]:
        """Get table configuration by table ID.

        Args:
            table_id: The table identifier
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            TableConfig if found, None otherwise
        """
        if user_timezone:
            return self.get_with_timezone(table_id, user_timezone=user_timezone)
        return self.get(table_id)

    def get_tables_by_pipeline(self, pipeline_id: str, user_timezone: Optional[str] = None) -> List[TableConfig]:
        """Get all table configurations for a specific pipeline.

        Args:
            pipeline_id: The pipeline identifier
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of TableConfig instances for the pipeline
        """
        if user_timezone:
            all_tables = self.list_all_with_timezone(user_timezone)
        else:
            all_tables = self.list_all()
        return [table for table in all_tables if table.pipeline_id == pipeline_id]

    def get_active_tables_by_pipeline(self, pipeline_id: str, user_timezone: Optional[str] = None) -> List[TableConfig]:
        """Get active table configurations for a specific pipeline.

        Args:
            pipeline_id: The pipeline identifier
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of active TableConfig instances for the pipeline
        """
        pipeline_tables = self.get_tables_by_pipeline(pipeline_id, user_timezone)
        return [table for table in pipeline_tables if table.is_active]

    def get_tables_by_type(self, table_type: TableType, pipeline_id: Optional[str] = None, user_timezone: Optional[str] = None) -> List[TableConfig]:
        """Get table configurations by table type.

        Args:
            table_type: The table type to filter by
            pipeline_id: Optional pipeline ID to further filter
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of TableConfig instances of the specified type
        """
        if user_timezone:
            all_tables = self.list_all_with_timezone(user_timezone)
        else:
            all_tables = self.list_all()
        filtered_tables = [table for table in all_tables if table.table_type == table_type]

        if pipeline_id:
            filtered_tables = [table for table in filtered_tables if table.pipeline_id == pipeline_id]

        return filtered_tables

    def get_source_tables(self, pipeline_id: str, user_timezone: Optional[str] = None) -> List[TableConfig]:
        """Get source tables for a pipeline.

        Args:
            pipeline_id: The pipeline identifier
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of source TableConfig instances
        """
        return self.get_tables_by_type(TableType.SOURCE, pipeline_id, user_timezone)

    def get_destination_tables(self, pipeline_id: str, user_timezone: Optional[str] = None) -> List[TableConfig]:
        """Get destination tables for a pipeline.

        Args:
            pipeline_id: The pipeline identifier
            user_timezone: Optional timezone to convert datetime fields to

        Returns:
            List of destination TableConfig instances
        """
        return self.get_tables_by_type(TableType.DESTINATION, pipeline_id, user_timezone)

    def update_table_statistics(
        self,
        table_id: str,
        record_count: Optional[int] = None,
        size_bytes: Optional[int] = None,
        last_updated_data: Optional[datetime] = None,
        current_timezone: Optional[str] = None
    ) -> TableConfig:
        """Update table statistics.

        Args:
            table_id: The table identifier
            record_count: New record count
            size_bytes: New size in bytes
            last_updated_data: When data was last updated
            current_timezone: Optional timezone to use for updated_at timestamp

        Returns:
            Updated TableConfig instance
        """
        table = self.get_or_raise(table_id)

        if record_count is not None:
            table.record_count = record_count
        if size_bytes is not None:
            table.size_bytes = size_bytes
        if last_updated_data is not None:
            table.last_updated_data = last_updated_data

        # Use timezone-aware datetime if timezone specified
        if current_timezone:
            from ..utils.timezone import now_in_tz
            table.updated_at = now_in_tz(current_timezone)
        else:
            table.updated_at = datetime.now(timezone.utc)

        return self.update(table)

    def create_table_config(
        self,
        table_id: str,
        pipeline_id: str,
        table_name: str,
        table_type: TableType,
        data_format: str,
        location: str,
        created_by: Optional[str] = None,
        current_timezone: Optional[str] = None,
        **kwargs
    ) -> TableConfig:
        """Create a new table configuration.

        Args:
            table_id: Unique table identifier
            pipeline_id: Pipeline this table belongs to
            table_name: Physical table name
            table_type: Type of table (source, destination, etc.)
            data_format: Data format (parquet, json, etc.)
            location: Physical location/path
            created_by: User creating the configuration
            current_timezone: Optional timezone to use for created_at timestamp
            **kwargs: Additional configuration parameters

        Returns:
            Created TableConfig instance
        """
        config_data = {
            "table_id": table_id,
            "pipeline_id": pipeline_id,
            "table_name": table_name,
            "table_type": table_type,
            "data_format": data_format,
            "location": location,
            "created_by": created_by,
            **kwargs
        }

        table_config = TableConfig(**config_data)

        # Use timezone context if specified
        if current_timezone:
            return self.create_with_timezone_context(table_config, current_timezone)
        else:
            return self.create(table_config)
