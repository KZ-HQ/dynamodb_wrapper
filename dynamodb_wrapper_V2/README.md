# 🚀 DynamoDB Wrapper Library V2

A comprehensive Python library that provides a **Command Query Responsibility Segregation (CQRS)** wrapper around AWS DynamoDB using boto3 and Pydantic, specifically designed for PySpark pipeline environments.

## ✨ Key Features

- 🏗️ **CQRS Architecture**: Separate optimized read and write operations with domain boundaries
- 📊 **Meta Class Single Source of Truth**: Model metadata centralized in domain model Meta classes
- ⚡ **Modern Python**: Python 3.9+ with native zoneinfo timezone support (no legacy compatibility)
- 🔄 **DynamoDB Compatibility**: Automatic boolean-to-string conversion for GSI key compatibility
- 🌍 **Timezone Compliance**: Strict UTC storage with handler-layer timezone conversion
- 🎯 **Flexible View Models**: Support partial data projections without validation errors
- 🚨 **Domain-Specific Exceptions**: Comprehensive error handling with detailed context
- 📝 **Proper Upsert Semantics**: Timestamp preservation and true upsert behavior
- 📈 **Pipeline Management**: Complete CQRS APIs for pipelines, tables, and run logs
- 🧪 **Comprehensive Test Coverage**: 230 total tests (218 unit + 12 integration) covering all critical scenarios and edge cases

## 🏛️ Architecture Overview

The V2 architecture implements clean **Command Query Responsibility Segregation (CQRS)** with domain-driven design, featuring:

- **🎯 CQRS Pattern**: Separate optimized read/write operations with domain boundaries
- **📊 Meta Class Single Source**: Model metadata centralized in domain model Meta classes  
- **🔄 DynamoDB Compatibility**: Automatic boolean-to-string conversion for GSI keys
- **🌍 Modern Python**: Python 3.9+ native zoneinfo timezone support
- **🧪 Comprehensive Testing**: 230 total tests (218 unit + 12 integration) covering all critical scenarios and edge cases

> **📖 For detailed architecture documentation, see [ARCHITECTURE.md](./ARCHITECTURE.md)**
> 
> Includes: Layer breakdown, data flow diagrams, performance optimizations, extension points, and compliance architecture.

## 📦 Installation

### Using uv (Recommended)

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone <repository-url>
cd dynamodb-wrapper-v2

# Create virtual environment and install dependencies
uv sync

# Install in development mode
uv pip install -e .
```

### Using pip

```bash
pip install -e .
```

## 🚀 Quick Start

### Basic CQRS Usage

```python
from dynamodb_wrapper import (
    DynamoDBConfig,
    PipelineConfigReadApi,
    PipelineConfigWriteApi,
    PipelineConfigUpsert
)

# Configuration
config = DynamoDBConfig.from_env()

# Read API - Optimized queries
read_api = PipelineConfigReadApi(config)

# Get pipeline with projection (reduced data transfer)
pipeline = read_api.get_by_id(
    "my-pipeline",
    projection=['pipeline_id', 'pipeline_name', 'is_active']
)

# Query active pipelines with pagination
active_pipelines, next_key = read_api.query_active_pipelines(
    limit=50,
    last_key=None
)

# Write API - Validated operations
write_api = PipelineConfigWriteApi(config)

# Create pipeline with validation
pipeline_data = PipelineConfigUpsert(
    pipeline_id="new-pipeline",
    pipeline_name="Data Processing Pipeline",
    source_type="s3",
    destination_type="warehouse"
)

created_pipeline = write_api.create_pipeline(pipeline_data)

# Update with conditional expression
write_api.update_pipeline(
    "new-pipeline",
    {"is_active": False},
    condition_expression=Attr('is_active').eq(True)  # Only if currently active
)
```

### Advanced Query Patterns

```python
from dynamodb_wrapper import PipelineRunLogsReadApi
from datetime import datetime, timezone

read_api = PipelineRunLogsReadApi(config)

# Time-range queries with composite keys
logs, next_key = read_api.query_by_composite_key_range(
    partition_key="pipeline_id",
    partition_value="my-pipeline",
    sort_key="created_at", 
    sort_condition="between",
    sort_value=datetime(2024, 1, 1, tzinfo=timezone.utc),
    sort_value2=datetime(2024, 1, 31, tzinfo=timezone.utc),
    limit=100
)

# Scan with filters and projections
all_failed_runs, _ = read_api.scan_for_all_runs(
    projection=['run_id', 'pipeline_id', 'status', 'error_message'],
    filters={'status': 'failed'},
    limit=50
)
```

### Flexible Queries and Projections

```python
from dynamodb_wrapper import TableConfigReadApi
from datetime import datetime, timezone

# Efficient queries with field projections
table_api = TableConfigReadApi(config)

# Get only specific fields to reduce data transfer
table_summary = table_api.get_by_id(
    "table-123",
    projection=['table_id', 'table_name', 'table_type', 'is_active']
)

# Query with filters and pagination
active_tables, next_token = table_api.query_by_pipeline(
    pipeline_id="my-pipeline",
    filters={'is_active': True, 'table_type': 'source'},
    limit=20,
    last_key=None
)

# Time-based queries with GSI
recent_configs, _ = table_api.query_recent_configs(
    since=datetime(2024, 1, 1, tzinfo=timezone.utc),
    limit=50
)
```

## 🌍 Timezone Management

The V2 library enforces strict timezone compliance with UTC-only storage and handler-layer conversion:

```python
from datetime import datetime, timezone
from dynamodb_wrapper.utils import to_utc, to_user_timezone

# Convert user input to UTC before storage
user_time = datetime.now(timezone.utc)
utc_time = to_utc(user_time)  # Handler responsibility

# Create run log with UTC time
run_log_data = PipelineRunLogUpsert(
    run_id="run-123",
    pipeline_id="my-pipeline",
    status="running",
    start_time=utc_time  # Always UTC at gateway
)

# Convert back to user timezone for display
display_time = to_user_timezone(utc_time, "America/New_York")
```

> **📖 For comprehensive timezone architecture and compliance rules, see [ARCHITECTURE.md](./ARCHITECTURE.md#-timezone-compliance-architecture)**

## ⚙️ Configuration

### Environment Variables

```bash
# AWS Configuration
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"

# DynamoDB Configuration  
export DYNAMODB_ENDPOINT_URL="http://localhost:8000"  # For local development
export DYNAMODB_TABLE_PREFIX="dev_"                   # Table name prefix

# Timezone Configuration
export DYNAMODB_USER_TIMEZONE="America/New_York"      # User display timezone
```

### Programmatic Configuration

```python
config = DynamoDBConfig(
    aws_access_key_id="your-key",
    aws_secret_access_key="your-secret", 
    region_name="us-east-1",
    table_prefix="prod_",
    user_timezone="America/New_York"
)
```


## 📊 Data Models

### Model Types

1. **Full Models** - Complete data models with all fields
2. **View Models** - Read-optimized models (50-80% smaller)  
3. **DTO Models** - Write-optimized models with validation

```python
from dynamodb_wrapper import (
    PipelineConfig,           # Full model
    PipelineConfigView,       # Read-optimized view
    PipelineConfigUpsert      # Write-optimized DTO
)

# View models for efficient reads
pipeline_view: PipelineConfigView = read_api.get_by_id("pipeline-1")

# DTOs for validated writes
pipeline_dto = PipelineConfigUpsert(
    pipeline_id="new-pipeline",
    pipeline_name="My Pipeline",
    source_type="s3",
    destination_type="warehouse"
)
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test categories
uv run pytest tests/unit/
uv run pytest tests/integration/

# Run with coverage
uv run pytest --cov=dynamodb_wrapper

# Test results: 230/230 tests passing ✅
# - Unit Tests: 218 passing  
# - Integration Tests: 4 passing
```

### Test Structure

- **Unit Tests** (`tests/unit/`): 218 tests covering individual components with comprehensive mocking
- **Integration Tests** (`tests/integration/`): 4 tests with actual DynamoDB operations  
- **Model-Driven Tests**: Meta class validation and key building tests
- **Timezone Tests**: Python 3.9+ zoneinfo compliance verification
- **CQRS Tests**: End-to-end read/write API validation
- **Edge Case Coverage**: Batch operations, error mapping, transaction atomicity, concurrent updates

## 🔧 Development

### Virtual Environment Setup

```bash
# Using uv (recommended)
uv sync --dev

# Using pip
pip install -r requirements.txt
pip install -e .
```

### Code Quality

```bash
# Format code
uv run ruff format .

# Lint code  
uv run ruff check .

# Type checking (if mypy is installed)
mypy dynamodb_wrapper
```

## 📈 DynamoDB Table Schema

The library manages three core DynamoDB tables:

### Pipeline Configuration
```
Table: pipeline_config
- Primary Key: pipeline_id (String)
- Attributes: pipeline_name, source_type, destination_type, is_active, etc.
```

### Table Configuration  
```
Table: table_config
- Primary Key: table_id (String)
- Attributes: pipeline_id, table_name, table_type, data_format, etc.
```

### Pipeline Run Logs
```
Table: pipeline_run_logs  
- Primary Key: run_id (String)
- Sort Key: pipeline_id (String)  # Composite key for run logs
- GSIs: PipelineRunsIndex, StatusRunsIndex
- Attributes: status, start_time, end_time, metrics, etc.
```

## 🚨 Error Handling

### Domain-Specific Exceptions

```python
from dynamodb_wrapper.exceptions import (
    ItemNotFoundError,
    ValidationError,
    ConflictError,
    ConnectionError,
    RetryableError
)

try:
    pipeline = read_api.get_by_id("non-existent")
except ItemNotFoundError:
    print("Pipeline not found")
except ConnectionError:
    print("DynamoDB connection issue")
```

## 📚 Examples

See the `examples/` directory for comprehensive usage examples:

- `examples/basic_usage.py` - Basic CQRS operations with current V2 APIs
- `examples/pyspark_usage.py` - Model-driven key management and CQRS patterns
- `examples/timezone_usage.py` - Modern timezone handling with Python 3.9+
- `examples/timezone_handling_demo.py` - Advanced timezone compliance patterns

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch  
3. Make your changes
4. Add tests
5. Ensure all tests pass
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues, questions, or contributions, please open an issue on the repository or contact the development team.

---

**Built with ❤️ for PySpark pipeline environments**