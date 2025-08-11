import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator

# Load environment variables from .env file if it exists
load_dotenv()

# Import at top level to avoid circular imports - only imported when method is called
# This is acceptable since get_timezone_manager is only called after config initialization


class DynamoDBConfig(BaseModel):
    """Configuration for DynamoDB connection and operations."""

    aws_access_key_id: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"),
        description="AWS access key ID"
    )

    aws_secret_access_key: Optional[str] = Field(
        default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"),
        description="AWS secret access key"
    )

    region_name: str = Field(
        default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"),
        description="AWS region name"
    )

    # DynamoDB specific settings
    endpoint_url: Optional[str] = Field(
        default_factory=lambda: os.getenv("DYNAMODB_ENDPOINT_URL"),
        description="DynamoDB endpoint URL (for local development)"
    )

    # Table configuration
    table_prefix: str = Field(
        default_factory=lambda: os.getenv("DYNAMODB_TABLE_PREFIX", ""),
        description="Prefix to add to all table names"
    )

    # Connection settings
    max_pool_connections: int = Field(
        default=50,
        description="Maximum number of connections in the connection pool"
    )

    retries: int = Field(
        default=3,
        description="Number of retry attempts for failed requests"
    )

    timeout_seconds: float = Field(
        default=30.0,
        description="Request timeout in seconds"
    )

    # Environment settings
    environment: str = Field(
        default_factory=lambda: os.getenv("ENVIRONMENT", "dev"),
        description="Current environment (dev, staging, prod)"
    )

    # Logging settings
    enable_debug_logging: bool = Field(
        default_factory=lambda: os.getenv("DYNAMODB_DEBUG_LOGGING", "false").lower() == "true",
        description="Enable debug logging for DynamoDB operations"
    )

    # Timezone settings
    default_timezone: str = Field(
        default_factory=lambda: os.getenv("DYNAMODB_TIMEZONE", "UTC"),
        description="Default timezone for datetime operations"
    )

    store_timestamps_in_utc: bool = Field(
        default=True,
        description="Store timestamps in UTC in DynamoDB (recommended)"
    )

    user_timezone: Optional[str] = Field(
        default_factory=lambda: os.getenv("DYNAMODB_USER_TIMEZONE"),
        description="User's preferred timezone for display purposes"
    )

    @field_validator('region_name')
    @classmethod
    def validate_region(cls, v):
        """Validate AWS region name."""
        if not v:
            raise ValueError("AWS region name is required")
        return v

    @field_validator('environment')
    @classmethod
    def validate_environment(cls, v):
        """Validate environment value."""
        valid_environments = ['dev', 'staging', 'prod']
        if v not in valid_environments:
            raise ValueError(f"Environment must be one of: {valid_environments}")
        return v

    @field_validator('default_timezone', 'user_timezone')
    @classmethod
    def validate_timezone(cls, v):
        """Validate timezone string."""
        if v is None:
            return v

        # Try to create ZoneInfo to validate timezone
        try:
            # Import here to avoid circular imports
            import sys
            if sys.version_info >= (3, 9):
                from zoneinfo import ZoneInfo
            else:
                try:
                    from backports.zoneinfo import ZoneInfo  # type: ignore
                except ImportError:
                    from zoneinfo import ZoneInfo  # type: ignore

            ZoneInfo(v)
            return v
        except Exception:
            # Fallback to basic validation for common timezones
            if v in ['UTC', 'GMT']:
                return v
            # Only accept timezone format that looks like IANA (Region/City)
            if '/' in v and len(v.split('/')) == 2 and v.replace('/', '').replace('_', '').replace('-', '').isalnum():
                # Basic format check passed, but warn that validation couldn't be performed
                import warnings
                warnings.warn(f"Could not validate timezone '{v}' - using basic format check only", stacklevel=2)
                return v
            raise ValueError(f"Invalid timezone: {v}. Please use a valid IANA timezone identifier.") from None

        return v

    def get_table_name(self, base_name: str) -> str:
        """Get the full table name with prefix and environment.

        Args:
            base_name: Base table name

        Returns:
            Full table name with prefix and environment
        """
        parts = []

        if self.table_prefix:
            parts.append(self.table_prefix)

        if self.environment != "prod":
            parts.append(self.environment)

        parts.append(base_name)

        return "_".join(parts)

    @classmethod
    def from_env(cls) -> 'DynamoDBConfig':
        """Create configuration from environment variables.

        Returns:
            DynamoDBConfig instance
        """
        return cls()

    @classmethod
    def for_local_development(cls) -> 'DynamoDBConfig':
        """Create configuration for local DynamoDB development.

        Returns:
            DynamoDBConfig instance configured for local development
        """
        return cls(
            aws_access_key_id="local",
            aws_secret_access_key="local",
            region_name="us-east-1",
            endpoint_url="http://localhost:8000",
            environment="dev",
            enable_debug_logging=True
        )

    @classmethod
    def for_pyspark(cls, spark_conf: dict = None) -> 'DynamoDBConfig':
        """Create configuration optimized for PySpark environment.

        Args:
            spark_conf: Optional Spark configuration dictionary

        Returns:
            DynamoDBConfig instance optimized for PySpark
        """
        config = cls()

        # Override with Spark configuration if provided
        if spark_conf:
            config.aws_access_key_id = spark_conf.get('spark.hadoop.fs.s3a.access.key', config.aws_access_key_id)
            config.aws_secret_access_key = spark_conf.get('spark.hadoop.fs.s3a.secret.key', config.aws_secret_access_key)
            config.region_name = spark_conf.get('spark.hadoop.fs.s3a.endpoint.region', config.region_name)
            config.endpoint_url = spark_conf.get('spark.hadoop.fs.s3a.endpoint', config.endpoint_url)

        # Optimize for PySpark workloads
        config.max_pool_connections = 100
        config.timeout_seconds = 60.0
        config.retries = 5

        return config

    @classmethod
    def with_timezone(cls, timezone_str: str, **kwargs) -> 'DynamoDBConfig':
        """Create configuration with specific timezone.

        Args:
            timezone_str: Timezone string (e.g., 'America/New_York')
            **kwargs: Additional configuration parameters

        Returns:
            DynamoDBConfig instance with timezone configuration
        """
        config = cls(**kwargs)
        config.default_timezone = timezone_str
        return config

    def get_timezone_manager(self):
        """Get a TimezoneManager configured with this config's settings.

        Returns:
            TimezoneManager instance (cached)
        """
        if not hasattr(self, '_timezone_manager') or self._timezone_manager is None:
            # Import here to avoid circular imports (config -> utils -> models -> config)
            from ..utils.timezone import TimezoneManager
            self._timezone_manager = TimezoneManager(self.default_timezone)
        return self._timezone_manager

    model_config = ConfigDict(
        validate_assignment=True,
        use_enum_values=True
    )
