import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import BaseModel

from ..config import DynamoDBConfig
from ..exceptions import ConnectionError, ItemNotFoundError, ValidationError

T = TypeVar('T', bound=BaseModel)

logger = logging.getLogger(__name__)


class BaseDynamoRepository(Generic[T], ABC):
    """Base repository class for DynamoDB operations with Pydantic models."""

    def __init__(self, config: DynamoDBConfig):
        """Initialize repository with DynamoDB configuration.

        Args:
            config: DynamoDB configuration object
        """
        self.config = config
        self._dynamodb = None
        self._table = None
        self._timezone_manager = None

    @property
    def dynamodb(self):
        """Lazy initialization of DynamoDB resource."""
        if self._dynamodb is None:
            try:
                session = boto3.Session(
                    aws_access_key_id=self.config.aws_access_key_id,
                    aws_secret_access_key=self.config.aws_secret_access_key,
                    region_name=self.config.region_name
                )

                # Configure connection parameters
                dynamodb_config = {
                    'region_name': self.config.region_name
                }

                if self.config.endpoint_url:
                    dynamodb_config['endpoint_url'] = self.config.endpoint_url

                # Add retry and timeout configuration
                boto_config = Config(
                    retries={'max_attempts': self.config.retries},
                    max_pool_connections=self.config.max_pool_connections,
                    read_timeout=self.config.timeout_seconds,
                    connect_timeout=self.config.timeout_seconds
                )
                dynamodb_config['config'] = boto_config

                self._dynamodb = session.resource('dynamodb', **dynamodb_config)
            except Exception as e:
                logger.error(f"Failed to create DynamoDB resource: {e}")
                raise ConnectionError(f"Failed to connect to DynamoDB: {e}", e) from e
        return self._dynamodb

    @property
    def table(self):
        """Lazy initialization of DynamoDB table."""
        if self._table is None:
            try:
                self._table = self.dynamodb.Table(self.table_name)
            except Exception as e:
                logger.error(f"Failed to access table '{self.table_name}': {e}")
                raise ConnectionError(f"Failed to access table '{self.table_name}': {e}", e) from e
        return self._table

    @property
    def timezone_manager(self):
        """Lazy initialization of timezone manager."""
        if self._timezone_manager is None:
            from ..utils.timezone import TimezoneManager
            self._timezone_manager = TimezoneManager(self.config.default_timezone)
        return self._timezone_manager

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Return the DynamoDB table name."""
        pass

    @property
    @abstractmethod
    def model_class(self) -> type[T]:
        """Return the Pydantic model class for this repository."""
        pass

    @property
    @abstractmethod
    def primary_key(self) -> str:
        """Return the primary key field name."""
        pass

    @property
    def sort_key(self) -> Optional[str]:
        """Return the sort key field name if exists."""
        return None

    def _item_to_model(self, item: Dict[str, Any]) -> T:
        """Convert DynamoDB item to Pydantic model."""
        try:
            # Convert ISO string datetime back to datetime objects
            def convert_datetime_strings(obj):
                if isinstance(obj, dict):
                    return {k: convert_datetime_strings(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_datetime_strings(item) for item in obj]
                elif isinstance(obj, str) and 'T' in obj and obj.count('-') >= 2:
                    # Try to parse as datetime
                    try:
                        dt = datetime.fromisoformat(obj.replace('Z', '+00:00'))
                        # Convert to user's preferred timezone if specified
                        if self.config.user_timezone:
                            dt = self.timezone_manager.to_timezone(dt, self.config.user_timezone)
                        return dt
                    except ValueError:
                        return obj
                else:
                    return obj

            converted_item = convert_datetime_strings(item)
            return self.model_class(**converted_item)
        except Exception as e:
            logger.error(f"Failed to convert item to model: {e}")
            raise ValidationError(f"Failed to convert item to model: {e}") from e

    def _model_to_item(self, model: T) -> Dict[str, Any]:
        """Convert Pydantic model to DynamoDB item."""
        item = model.model_dump(exclude_none=True)

        # Convert datetime objects to ISO strings for DynamoDB
        def convert_datetime(obj):
            if isinstance(obj, dict):
                return {k: convert_datetime(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime(item) for item in obj]
            elif isinstance(obj, datetime):
                # Ensure timezone-aware and convert to storage format
                from ..utils.timezone import ensure_timezone_aware, to_utc
                dt = ensure_timezone_aware(obj, self.config.default_timezone)
                if self.config.store_timestamps_in_utc:
                    dt = to_utc(dt)
                return dt.isoformat()
            else:
                return obj

        return convert_datetime(item)

    def _get_key(self, pk_value: Any, sk_value: Any = None) -> Dict[str, Any]:
        """Build key dictionary for DynamoDB operations."""
        key = {self.primary_key: pk_value}
        if self.sort_key and sk_value is not None:
            key[self.sort_key] = sk_value
        return key

    def create(self, model: T) -> T:
        """Create a new item in DynamoDB.

        Args:
            model: Pydantic model instance to create

        Returns:
            The created model instance

        Raises:
            ValidationError: If model validation fails
            ConnectionError: If DynamoDB operation fails
        """
        try:
            item = self._model_to_item(model)
            self.table.put_item(Item=item)
            logger.info(f"Created item in {self.table_name}: {item}")
            return model
        except ClientError as e:
            logger.error(f"Failed to create item in {self.table_name}: {e}")
            raise ConnectionError(f"Failed to create item: {e}", e) from e

    def get(self, pk_value: Any, sk_value: Any = None) -> Optional[T]:
        """Get an item by primary key (and sort key if applicable).

        Args:
            pk_value: Primary key value
            sk_value: Sort key value (if table has sort key)

        Returns:
            Model instance if found, None otherwise

        Raises:
            ConnectionError: If DynamoDB operation fails
        """
        try:
            key = self._get_key(pk_value, sk_value)
            response = self.table.get_item(Key=key)

            if 'Item' in response:
                return self._item_to_model(response['Item'])
            return None

        except ClientError as e:
            logger.error(f"Failed to get item from {self.table_name}: {e}")
            raise ConnectionError(f"Failed to get item: {e}", e) from e

    def get_or_raise(self, pk_value: Any, sk_value: Any = None) -> T:
        """Get an item or raise ItemNotFoundError if not found.

        Args:
            pk_value: Primary key value
            sk_value: Sort key value (if table has sort key)

        Returns:
            Model instance

        Raises:
            ItemNotFoundError: If item not found
            ConnectionError: If DynamoDB operation fails
        """
        item = self.get(pk_value, sk_value)
        if item is None:
            key = self._get_key(pk_value, sk_value)
            raise ItemNotFoundError(self.table_name, key)
        return item

    def update(self, model: T) -> T:
        """Update an existing item in DynamoDB.

        Args:
            model: Pydantic model instance to update

        Returns:
            The updated model instance

        Raises:
            ValidationError: If model validation fails
            ConnectionError: If DynamoDB operation fails
        """
        try:
            item = self._model_to_item(model)
            self.table.put_item(Item=item)
            logger.info(f"Updated item in {self.table_name}: {item}")
            return model
        except ClientError as e:
            logger.error(f"Failed to update item in {self.table_name}: {e}")
            raise ConnectionError(f"Failed to update item: {e}", e) from e

    def delete(self, pk_value: Any, sk_value: Any = None) -> bool:
        """Delete an item by primary key (and sort key if applicable).

        Args:
            pk_value: Primary key value
            sk_value: Sort key value (if table has sort key)

        Returns:
            True if item was deleted, False if item didn't exist

        Raises:
            ConnectionError: If DynamoDB operation fails
        """
        try:
            key = self._get_key(pk_value, sk_value)
            response = self.table.delete_item(
                Key=key,
                ReturnValues='ALL_OLD'
            )

            deleted = 'Attributes' in response
            if deleted:
                logger.info(f"Deleted item from {self.table_name}: {key}")
            return deleted

        except ClientError as e:
            logger.error(f"Failed to delete item from {self.table_name}: {e}")
            raise ConnectionError(f"Failed to delete item: {e}", e) from e

    def list_all(self) -> List[T]:
        """List all items in the table.

        Returns:
            List of model instances

        Raises:
            ConnectionError: If DynamoDB operation fails
        """
        try:
            response = self.table.scan()
            items = []

            for item in response.get('Items', []):
                items.append(self._item_to_model(item))

            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for item in response.get('Items', []):
                    items.append(self._item_to_model(item))

            logger.info(f"Retrieved {len(items)} items from {self.table_name}")
            return items

        except ClientError as e:
            logger.error(f"Failed to scan table {self.table_name}: {e}")
            raise ConnectionError(f"Failed to scan table: {e}", e) from e

    def query_by_pk(self, pk_value: Any, **kwargs) -> List[T]:
        """Query items by primary key.

        Args:
            pk_value: Primary key value
            **kwargs: Additional query parameters

        Returns:
            List of model instances

        Raises:
            ConnectionError: If DynamoDB operation fails
        """
        try:
            query_kwargs = {
                'KeyConditionExpression': Key(self.primary_key).eq(pk_value),
                **kwargs
            }

            response = self.table.query(**query_kwargs)
            items = []

            for item in response.get('Items', []):
                items.append(self._item_to_model(item))

            # Handle pagination
            while 'LastEvaluatedKey' in response:
                query_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                response = self.table.query(**query_kwargs)
                for item in response.get('Items', []):
                    items.append(self._item_to_model(item))

            logger.info(f"Query returned {len(items)} items from {self.table_name}")
            return items

        except ClientError as e:
            logger.error(f"Failed to query table {self.table_name}: {e}")
            raise ConnectionError(f"Failed to query table: {e}", e) from e

    def get_with_timezone(self, pk_value: Any, sk_value: Any = None, user_timezone: Optional[str] = None) -> Optional[T]:
        """Get an item and convert datetime fields to specified timezone.

        Args:
            pk_value: Primary key value
            sk_value: Sort key value (if table has sort key)
            user_timezone: Timezone to convert datetime fields to

        Returns:
            Model instance with datetime fields in user timezone, None if not found
        """
        # Temporarily override user timezone
        original_tz = self.config.user_timezone
        if user_timezone:
            self.config.user_timezone = user_timezone

        try:
            return self.get(pk_value, sk_value)
        finally:
            self.config.user_timezone = original_tz

    def list_all_with_timezone(self, user_timezone: Optional[str] = None) -> List[T]:
        """List all items with datetime fields converted to specified timezone.

        Args:
            user_timezone: Timezone to convert datetime fields to

        Returns:
            List of model instances with datetime fields in user timezone
        """
        # Temporarily override user timezone
        original_tz = self.config.user_timezone
        if user_timezone:
            self.config.user_timezone = user_timezone

        try:
            return self.list_all()
        finally:
            self.config.user_timezone = original_tz

    def create_with_timezone_context(
        self,
        model: T,
        current_timezone: Optional[str] = None
    ) -> T:
        """Create an item with timezone context for datetime fields.

        Args:
            model: Pydantic model instance to create
            current_timezone: Timezone to assume for naive datetime fields in the model

        Returns:
            The created model instance
        """
        # Update timezone manager temporarily if needed
        if current_timezone:
            original_tz = self.timezone_manager.default_timezone
            self.timezone_manager.default_timezone = current_timezone
            try:
                return self.create(model)
            finally:
                self.timezone_manager.default_timezone = original_tz
        else:
            return self.create(model)
