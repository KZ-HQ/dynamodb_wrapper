# 🚀 DynamoDB Wrapper Library

A comprehensive Python library that provides a repository pattern wrapper around AWS DynamoDB using boto3 and Pydantic, specifically designed for PySpark pipeline environments.

## ✨ Features

- 🏗️ **Repository Pattern**: Clean abstraction layer over DynamoDB operations
- 📊 **Pydantic Models**: Type-safe data validation and serialization
- ⚡ **PySpark Integration**: Utilities and helpers for Spark pipeline environments  
- 🌍 **Timezone Support**: Global timezone handling with UTC storage and user-specific display
- ⚙️ **Configuration Management**: Flexible configuration with environment variable support
- 🚨 **Error Handling**: Comprehensive exception handling with detailed error messages
- 📝 **Logging**: Built-in logging for monitoring and debugging
- 📈 **Pipeline Management**: Models and repositories for pipeline configurations, table metadata, and run logs

## 📦 Installation

### Using uv (Recommended)

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone <repository-url>
cd dynamodb-wrapper

# Create virtual environment and install dependencies
uv sync

# Activate the virtual environment
source .venv/bin/activate
```

### Using pip

```bash
pip install -e .
```

## 🚀 Quick Start

### 💡 Basic Usage

```python
from dynamodb_wrapper_V1.dynamodb_wrapper import (
    DynamoDBConfig,
    PipelineConfigRepository,
    TableConfigRepository,
    PipelineRunLogsRepository
)

# Configure DynamoDB connection
config = DynamoDBConfig.from_env()  # Uses environment variables

# Initialize repositories
pipeline_repo = PipelineConfigRepository(config)
table_repo = TableConfigRepository(config)
logs_repo = PipelineRunLogsRepository(config)

# Create a pipeline configuration
pipeline = pipeline_repo.create_pipeline_config(
    pipeline_id="sales-pipeline",
    pipeline_name="Sales Analytics Pipeline",
    source_type="s3",
    destination_type="redshift",
    created_by="data_engineer"
)

# Create table configuration
from dynamodb_wrapper_V1.dynamodb_wrapper.models import TableType

table = table_repo.create_table_config(
    table_id="sales-data",
    pipeline_id="sales-pipeline",
    table_name="sales_raw",
    table_type=TableType.SOURCE,
    data_format="parquet",
    location="s3://bucket/sales/"
)

# Create run log
run = logs_repo.create_run_log(
    run_id="run-001",
    pipeline_id="sales-pipeline",
    trigger_type="schedule"
)
```

### ⚡ PySpark Integration

```python
from dynamodb_wrapper_V1.dynamodb_wrapper.utils import SparkDynamoDBIntegration

# Initialize integration
config = DynamoDBConfig.for_pyspark()
integration = SparkDynamoDBIntegration(config)

# Use context manager for automatic run logging
with integration.pipeline_run_context(
    pipeline_id="sales-pipeline",
    trigger_type="manual"
) as run_id:
    
    # Create Spark session with pipeline configuration
    spark = integration.create_spark_session("sales-pipeline")
    
    # Get table configurations
    read_options = integration.get_table_read_options("sales-data")
    write_options = integration.get_table_write_options("processed-data")
    
    # Process data
    df = spark.read.options(**read_options).parquet("s3://bucket/input/")
    processed_df = df.groupBy("category").sum("amount")
    
    # Write results
    processed_df.write.options(**write_options).parquet("s3://bucket/output/")
    
    # Update table statistics
    integration.update_table_stats_after_write("processed-data", processed_df, run_id)
```

## ⚙️ Configuration

### 🌍 Environment Variables

```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1

# DynamoDB Configuration
DYNAMODB_ENDPOINT_URL=http://localhost:8000  # For local development
DYNAMODB_TABLE_PREFIX=myapp
DYNAMODB_DEBUG_LOGGING=true

# Timezone Configuration
DYNAMODB_TIMEZONE=UTC                    # Default timezone for operations
DYNAMODB_USER_TIMEZONE=America/New_York  # User display timezone

# Environment
ENVIRONMENT=dev  # dev, staging, prod
```

### 🔧 Programmatic Configuration

```python
# For production
config = DynamoDBConfig(
    aws_access_key_id="your_key",
    aws_secret_access_key="your_secret",
    region_name="us-east-1",
    environment="prod"
)

# For local development
config = DynamoDBConfig.for_local_development()

# For PySpark environments
config = DynamoDBConfig.for_pyspark()

# With timezone configuration
config = DynamoDBConfig.with_timezone(
    "Europe/London",
    store_timestamps_in_utc=True,
    user_timezone="America/New_York"
)
```

## 🌍 Timezone Support

The library provides comprehensive timezone support for global applications:

### 🔧 Configuration

```python
from dynamodb_wrapper_V1.dynamodb_wrapper import DynamoDBConfig
from dynamodb_wrapper_V1.dynamodb_wrapper.utils import set_global_timezone, now_in_tz

# Configure default timezone
config = DynamoDBConfig(
    default_timezone="UTC",           # Storage timezone
    user_timezone="Europe/London",   # Display timezone
    store_timestamps_in_utc=True     # Recommended
)

# Set global timezone
set_global_timezone("America/Chicago")
```

### 💡 Usage with Optional Parameters

```python
# Get current time in specific timezone
london_time = now_in_tz("Europe/London")
tokyo_time = now_in_tz("Asia/Tokyo")

# Repository operations with optional timezone parameter
pipeline = pipeline_repo.get_by_pipeline_id(
    "pipeline-id", 
    user_timezone="Australia/Sydney"  # Optional timezone conversion
)

# Create with timezone context
pipeline = pipeline_repo.create_pipeline_config(
    "new-pipeline",
    "Analytics Pipeline",
    "s3",
    "redshift",
    created_by="analyst",
    current_timezone="America/New_York"  # Optional timezone for timestamps
)

# Get active pipelines with timezone conversion
pipelines = pipeline_repo.get_active_pipelines(user_timezone="Europe/Berlin")
```

### 📋 Best Practices

- 🕒 Store all timestamps in UTC in DynamoDB
- 🌏 Convert to user's timezone only for display
- ⏰ Use timezone-aware datetime objects
- 👤 Configure user timezone per request/user
- 🧪 Test with multiple timezones including DST transitions

## 📊 Models

### PipelineConfig
Stores pipeline configuration including source/destination types, Spark settings, scheduling, and metadata.

### TableConfig  
Stores table metadata including schema definitions, partition columns, data formats, and processing options.

### PipelineRunLog
Tracks pipeline execution with status, timing, resource usage, data quality results, and error information.

## 🏗️ Repository Operations

All repositories support standard CRUD operations:

- 📝 `create(model)` - Create new item
- 🔍 `get(primary_key)` - Get item by primary key
- ⚡ `get_or_raise(primary_key)` - Get item or raise ItemNotFoundError
- 🔄 `update(model)` - Update existing item
- 🗑️ `delete(primary_key)` - Delete item
- 📋 `list_all()` - Get all items
- 🔎 `query_by_pk(primary_key)` - Query items by primary key

### 🎯 Specialized Repository Methods

#### 📈 PipelineConfigRepository
- `get_active_pipelines(user_timezone=None)` - Get all active pipelines
- `get_pipelines_by_environment(env, user_timezone=None)` - Filter by environment
- `update_pipeline_status(pipeline_id, is_active, updated_by, current_timezone=None)` - Update active status

#### 📊 TableConfigRepository  
- `get_tables_by_pipeline(pipeline_id, user_timezone=None)` - Get tables for pipeline
- `get_source_tables(pipeline_id, user_timezone=None)` - Get source tables
- `get_destination_tables(pipeline_id, user_timezone=None)` - Get destination tables
- `update_table_statistics(table_id, record_count, size_bytes, last_updated_data)` - Update table stats

#### 📋 PipelineRunLogsRepository
- `get_runs_by_pipeline(pipeline_id, limit=None, user_timezone=None)` - Get runs for pipeline
- `get_running_pipelines(user_timezone=None)` - Get currently running pipelines
- `get_failed_runs(pipeline_id, hours=24, user_timezone=None)` - Get recent failed runs
- `update_run_status(id, status, error_message=None, end_time=None)` - Update run status

## ⚡ PySpark Utilities

### 🔧 SparkDynamoDBIntegration
Main integration class providing:

- `create_spark_session(pipeline_id)` - Create configured Spark session
- `get_table_read_options(table_id)` - Get read options from table config
- `get_table_write_options(table_id)` - Get write options from table config
- `pipeline_run_context(pipeline_id)` - Context manager for run logging
- `update_table_stats_after_write()` - Update stats after data write

### 🔧 Standalone Functions
- `create_spark_session_with_dynamodb()` - Create Spark session with AWS config
- `get_pipeline_config_for_spark()` - Get pipeline config for Spark usage
- `get_table_configs_for_spark()` - Get table configs for Spark usage

## 🚨 Error Handling

The library provides comprehensive error handling:

- 🔥 `DynamoDBWrapperError` - Base exception class
- 🔍 `ItemNotFoundError` - Item not found in DynamoDB
- ⚠️ `ValidationError` - Pydantic model validation failed
- 🌐 `ConnectionError` - DynamoDB connection issues

## 🧪 Testing

Run the test suite:

### Using uv (Recommended)

```bash
# Run all tests
uv run pytest

# Run unit tests only  
uv run pytest tests/unit/

# Run with coverage
uv run pytest --cov=dynamodb_wrapper

# Run linting
uv run ruff check .
uv run black --check .
uv run mypy dynamodb_wrapper/
```

### Using activated environment

```bash
# Activate environment first
source .venv/bin/activate

# Run all tests
pytest

# Run unit tests only
pytest tests/unit/

# Run with coverage
pytest --cov=dynamodb_wrapper
```

## 📚 Examples

See the `examples/` directory for comprehensive usage examples:

- 💡 `basic_usage.py` - Basic CRUD operations and repository usage
- ⚡ `pyspark_usage.py` - PySpark integration examples with context managers
- 🌍 `timezone_usage.py` - Comprehensive timezone support examples

## 🗃️ DynamoDB Table Schema

The library expects the following DynamoDB tables:

### 📈 pipeline_config
- 🔑 Primary Key: `pipeline_id` (String)

### 📊 table_config  
- 🔑 Primary Key: `table_id` (String)

### 📋 pipeline_run_logs
- 🔑 Primary Key: `run_id` (String)

## 🤝 Contributing

1. 🍴 Fork the repository
2. 🌟 Create a feature branch
3. 🧪 Add tests for new functionality
4. ✅ Ensure all tests pass
5. 📬 Submit a pull request

## 📄 License

MIT License - see LICENSE file for details.

## 📝 Changelog

### Version 1.0.1 (Latest)

#### 🔧 **Bug Fixes & Improvements**
- **Fixed repository table names**: All repository `table_name` properties now correctly use configuration prefixes (e.g., `dev_pipeline_config` instead of `pipeline_config`)
- **Enhanced PySpark configuration**: Fixed `DynamoDBConfig.for_pyspark()` to read correct Spark configuration keys (`spark.hadoop.fs.s3a.*`)
- **Improved connection management**: Added proper connection configuration with retry logic, timeouts, and connection pooling to base repository
- **Timezone manager caching**: Implemented caching for `get_timezone_manager()` to improve performance
- **Updated datetime usage**: Replaced deprecated `datetime.utcnow()` with `datetime.now(timezone.utc)` in PySpark integration

#### ⚡ **Enhanced Exception System**
- **Rich exception attributes**: All exceptions now include `message`, `original_error`, and `context` attributes as documented
- **Better error context**: Enhanced `ConnectionError`, `ItemNotFoundError`, and `ValidationError` with structured context information
- **Improved error messages**: Added contextual information to exception string representations

#### 🧪 **Test Improvements**
- **Updated test expectations**: Fixed tests to reflect correct table naming with environment prefixes
- **Removed pytest warnings**: Fixed `TestRepository` class naming to prevent pytest collection warnings

#### 📚 **Documentation Updates**
- **Architecture documentation**: Added comprehensive ARCHITECTURE.md with detailed system design
- **Enhanced error handling docs**: Updated exception hierarchy documentation to match implementation
- **Configuration examples**: Improved PySpark configuration examples with correct parameter names