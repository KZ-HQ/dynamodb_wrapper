import os
from unittest.mock import patch

import pytest

from dynamodb_wrapper_V2.dynamodb_wrapper.config import DynamoDBConfig


class TestDynamoDBConfig:
    """Test cases for DynamoDBConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        with patch.dict(os.environ, {"AWS_REGION": "us-west-2"}):
            config = DynamoDBConfig()

            assert config.region_name == "us-west-2"
            assert config.max_pool_connections == 50
            assert config.retries == 3
            assert config.timeout_seconds == 30.0
            assert config.environment == "dev"

    def test_config_from_env_vars(self):
        """Test configuration from environment variables."""
        env_vars = {
            "AWS_ACCESS_KEY_ID": "test_key",
            "AWS_SECRET_ACCESS_KEY": "test_secret",
            "AWS_REGION": "eu-west-1",
            "DYNAMODB_ENDPOINT_URL": "http://localhost:8000",
            "DYNAMODB_TABLE_PREFIX": "test",
            "ENVIRONMENT": "staging"
        }

        with patch.dict(os.environ, env_vars):
            config = DynamoDBConfig()

            assert config.aws_access_key_id == "test_key"
            assert config.aws_secret_access_key == "test_secret"
            assert config.region_name == "eu-west-1"
            assert config.endpoint_url == "http://localhost:8000"
            assert config.table_prefix == "test"
            assert config.environment == "staging"

    def test_table_name_generation(self):
        """Test table name generation with prefix and environment."""
        config = DynamoDBConfig(
            table_prefix="myapp",
            environment="dev"
        )

        table_name = config.get_table_name("users")
        assert table_name == "myapp_dev_users"

    def test_table_name_generation_prod(self):
        """Test table name generation in production (no environment suffix)."""
        config = DynamoDBConfig(
            table_prefix="myapp",
            environment="prod"
        )

        table_name = config.get_table_name("users")
        assert table_name == "myapp_users"

    def test_table_name_generation_no_prefix(self):
        """Test table name generation without prefix."""
        config = DynamoDBConfig(environment="dev")

        table_name = config.get_table_name("users")
        assert table_name == "dev_users"

    def test_local_development_config(self):
        """Test local development configuration."""
        config = DynamoDBConfig.for_local_development()

        assert config.aws_access_key_id == "local"
        assert config.aws_secret_access_key == "local"
        assert config.endpoint_url == "http://localhost:8000"
        assert config.environment == "dev"
        assert config.enable_debug_logging is True

    def test_pyspark_config(self):
        """Test PySpark optimized configuration."""
        config = DynamoDBConfig.for_pyspark()

        assert config.max_pool_connections == 100
        assert config.timeout_seconds == 60.0
        assert config.retries == 5

    def test_pyspark_config_with_spark_conf(self):
        """Test PySpark configuration with Spark conf override."""
        spark_conf = {
            "spark.hadoop.fs.s3a.endpoint.region": "ap-south-1"
        }

        config = DynamoDBConfig.for_pyspark(spark_conf)

        assert config.region_name == "ap-south-1"
        assert config.max_pool_connections == 100

    def test_environment_validation(self):
        """Test environment validation."""
        with pytest.raises(ValueError, match="Environment must be one of"):
            DynamoDBConfig(environment="invalid")

    def test_region_validation(self):
        """Test region validation."""
        with pytest.raises(ValueError, match="AWS region name is required"):
            DynamoDBConfig(region_name="")
