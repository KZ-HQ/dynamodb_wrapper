# 🏗️ DynamoDB Wrapper Library Architecture

This document provides a comprehensive overview of the DynamoDB Wrapper Library architecture, design patterns, and implementation details.

## 📋 Table of Contents

- [Overview](#-overview)
- [Core Design Patterns](#-core-design-patterns)
- [Project Structure](#-project-structure)
- [Core Components](#-core-components)
- [Data Flow](#-data-flow)
- [Configuration System](#-configuration-system)
- [Error Handling Strategy](#-error-handling-strategy)
- [Testing Architecture](#-testing-architecture)
- [PySpark Integration](#-pyspark-integration)
- [Timezone Management](#-timezone-management)
- [Extension Points](#-extension-points)

## 🎯 Overview

The DynamoDB Wrapper Library is designed as a comprehensive data access layer that provides a clean abstraction over AWS DynamoDB operations. It follows the Repository pattern and leverages Pydantic for type-safe data validation, making it particularly suitable for PySpark pipeline environments.

### Key Design Principles

- **Repository Pattern**: Clean separation between data access and business logic
- **Type Safety**: Pydantic models ensure data integrity and validation
- **Configuration Flexibility**: Support for multiple deployment environments
- **Timezone Awareness**: Global timezone handling with UTC storage
- **Error Resilience**: Comprehensive exception handling and logging
- **PySpark Integration**: Native support for Spark pipeline workflows

## 🏛️ Core Design Patterns

### Repository Pattern

The library implements the Repository pattern to provide a consistent interface for data access operations:

```python
BaseDynamoRepository[T] (Abstract Base)
├── PipelineConfigRepository
├── TableConfigRepository  
└── PipelineRunLogsRepository
```

### Generic Type System

Uses Python generics to ensure type safety across all repository operations:

```python
T = TypeVar('T', bound=BaseModel)
class BaseDynamoRepository(Generic[T], ABC):
```

### Factory Pattern

Configuration objects use factory methods for common scenarios:

```python
DynamoDBConfig.for_local_development()
DynamoDBConfig.for_pyspark()  
DynamoDBConfig.from_env()
```

### Context Manager Pattern

PySpark integration provides context managers for automatic resource management:

```python
with integration.pipeline_run_context(pipeline_id, trigger_type) as run_id:
    # Pipeline operations
```

## 📁 Project Structure

```
dynamodb_wrapper/
├── __init__.py                 # Public API exports
├── config/
│   ├── __init__.py
│   └── config.py              # Configuration management
├── exceptions/
│   ├── __init__.py
│   ├── base.py               # Base exception class
│   ├── connection.py         # Connection-related exceptions
│   ├── item_not_found.py     # Item not found exceptions
│   └── validation.py         # Validation exceptions
├── models/
│   ├── __init__.py
│   ├── pipeline_config.py    # Pipeline configuration model
│   ├── pipeline_run_log.py   # Pipeline run logging model
│   └── table_config.py       # Table configuration model
├── repositories/
│   ├── __init__.py
│   ├── base.py              # Abstract base repository
│   ├── pipeline_config.py    # Pipeline config repository
│   ├── pipeline_run_logs.py  # Run logs repository
│   └── table_config.py       # Table config repository
└── utils/
    ├── __init__.py
    ├── pyspark_integration.py # PySpark utilities
    └── timezone.py           # Timezone management
```

## ⚙️ Core Components

### 1. Configuration System (`config/`)

**Purpose**: Centralized configuration management with environment-specific settings.

**Key Classes**:
- `DynamoDBConfig`: Main configuration class with AWS credentials, region, and environment settings

**Features**:
- Environment variable integration
- Factory methods for common scenarios
- Timezone configuration support
- Validation of required parameters

### 2. Models (`models/`)

**Purpose**: Pydantic-based data models ensuring type safety and validation.

**Key Models**:
- `PipelineConfig`: Pipeline configuration and metadata
- `TableConfig`: Table schema and processing configuration  
- `PipelineRunLog`: Execution logs and metrics
- Supporting enums: `RunStatus`, `LogLevel`, `TableType`

**Features**:
- Automatic datetime serialization/deserialization
- Timezone-aware datetime handling
- Field validation and type conversion
- JSON schema generation

### 3. Repository Layer (`repositories/`)

**Purpose**: Data access layer implementing the Repository pattern.

**Architecture**:
```python
BaseDynamoRepository[T] (Abstract)
├── Provides: CRUD operations, connection management, error handling
├── Abstract methods: table_name, model_class, primary_key, sort_key
└── Concrete implementations:
    ├── PipelineConfigRepository
    ├── TableConfigRepository
    └── PipelineRunLogsRepository
```

**Key Features**:
- Generic type safety
- Lazy connection initialization
- Automatic model conversion
- Error handling and logging
- Timezone-aware operations

### 4. Exception System (`exceptions/`)

**Purpose**: Hierarchical exception system for comprehensive error handling.

**Architecture**:
```python
DynamoDBWrapperError (Base)
├── ConnectionError       # AWS/DynamoDB connection issues
├── ItemNotFoundError    # Item not found in database
└── ValidationError      # Pydantic model validation failures
```

### 5. Utilities (`utils/`)

**Purpose**: Supporting utilities for specialized functionality.

**Components**:
- `SparkDynamoDBIntegration`: PySpark integration utilities
- `timezone`: Global timezone management functions

## 🔄 Data Flow

### 1. Basic Repository Operation Flow

```
Client Request
    ↓
Repository Method
    ↓
Model Validation (Pydantic)
    ↓
DynamoDB Item Conversion
    ↓
AWS DynamoDB API Call
    ↓
Response Processing
    ↓
Model Creation (Pydantic)
    ↓
Timezone Conversion (if specified)
    ↓
Return to Client
```

### 2. PySpark Integration Flow

```
Pipeline Trigger
    ↓
Context Manager Entry
    ↓
Run Log Creation
    ↓
Spark Session Creation
    ↓
Table Configuration Retrieval
    ↓
Data Processing (Spark)
    ↓
Statistics Update
    ↓
Run Status Update
    ↓
Context Manager Exit
```

## ⚙️ Configuration System

### Configuration Hierarchy

1. **Environment Variables** (highest priority)
2. **Programmatic Configuration**
3. **Factory Method Defaults** (lowest priority)

### Key Configuration Areas

```python
# AWS Connection
aws_access_key_id, aws_secret_access_key, region_name

# DynamoDB Settings  
endpoint_url, table_prefix

# Environment
environment (dev/staging/prod)

# Timezone
default_timezone, user_timezone, store_timestamps_in_utc

# Debugging
debug_logging
```

### Factory Methods

```python
# Local development with DynamoDB Local
config = DynamoDBConfig.for_local_development()

# PySpark environment with optimized settings
config = DynamoDBConfig.for_pyspark()

# Environment variable based configuration
config = DynamoDBConfig.from_env()

# Timezone-specific configuration
config = DynamoDBConfig.with_timezone("Europe/London")
```

## 🚨 Error Handling Strategy

### Exception Hierarchy

The library implements a comprehensive exception hierarchy:

```python
DynamoDBWrapperError (Base)
├── Attributes: message, original_error, context
├── Methods: __str__(), __repr__() with context information
├── ConnectionError
│   ├── Use: AWS connection failures, network issues
│   └── Context: endpoint_url, region, connection details
├── ItemNotFoundError  
│   ├── Use: DynamoDB item not found
│   └── Context: table_name, key values, query details
└── ValidationError
    ├── Use: Pydantic model validation failures
    └── Context: validation_errors with field-level details
```

### Error Handling Patterns

1. **Connection Errors**: Automatic retry logic and connection pooling
2. **Validation Errors**: Detailed field-level error reporting  
3. **Item Not Found**: Explicit vs implicit handling (`get()` vs `get_or_raise()`)
4. **Logging**: Structured logging with context information

## 🧪 Testing Architecture

### Test Organization

```
tests/
├── unit/                    # Unit tests
│   ├── test_base_repository.py
│   ├── test_concrete_repositories.py  
│   ├── test_config.py
│   ├── test_models.py
│   └── test_timezone.py
├── integration/            # Integration tests
└── conftest.py            # Pytest configuration
```

### Testing Patterns

1. **Mocking Strategy**: Mock AWS services using `moto` library
2. **Fixture System**: Reusable test fixtures for configuration and data
3. **Parameterized Tests**: Test multiple scenarios with parameters
4. **Coverage**: Comprehensive test coverage with pytest-cov

### Test Configuration

```python
# pyproject.toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
addopts = ["--strict-markers", "--verbose"]
```

## ⚡ PySpark Integration

### Integration Components

1. **SparkDynamoDBIntegration**: Main integration class
2. **Context Managers**: Automatic run logging and resource management
3. **Configuration Bridge**: DynamoDB config to Spark configuration
4. **Statistics Integration**: Automatic table statistics updates

### Key Integration Points

```python
# Spark Session Creation
spark = integration.create_spark_session(pipeline_id)

# Table Configuration Retrieval
read_options = integration.get_table_read_options(table_id)
write_options = integration.get_table_write_options(table_id)

# Automatic Run Logging
with integration.pipeline_run_context(pipeline_id, trigger) as run_id:
    # Spark operations
```

### Performance Optimizations

- **Connection pooling**: Configurable pool sizes for DynamoDB connections (`max_pool_connections`)
- **Retry logic**: Configurable retry attempts with exponential backoff (`retries`)
- **Timeout management**: Separate read and connect timeout configuration (`timeout_seconds`)
- **Lazy initialization**: DynamoDB resources and Spark sessions initialized on first use
- **Caching**: Timezone manager instances cached per configuration
- **Batch operations**: Statistics updates performed in batches where possible

## 🌍 Timezone Management

### Architecture Principles

1. **UTC Storage**: All timestamps stored in UTC in DynamoDB
2. **Display Conversion**: Convert to user timezone only for display
3. **Global Configuration**: System-wide timezone defaults
4. **Per-Operation Override**: Optional timezone parameter on methods
5. **DST Handling**: Proper daylight saving time transitions

### Implementation

```python
# Global timezone configuration
set_global_timezone("America/Chicago")

# Per-operation timezone
pipeline = repo.get_by_pipeline_id(
    "pipeline-id",
    user_timezone="Europe/London"  # Optional override
)

# Timezone-aware creation
pipeline = repo.create_pipeline_config(
    ...,
    current_timezone="Asia/Tokyo"  # Optional context
)
```

### Timezone Utilities

```python
# Get current time in specific timezone
london_time = now_in_tz("Europe/London")

# Convert UTC to user timezone  
user_time = convert_utc_to_user_tz(utc_time, "Australia/Sydney")

# Validate timezone
validate_timezone_string("America/New_York")
```

## 🔧 Extension Points

### Adding New Models

1. Create Pydantic model in `models/`
2. Implement repository in `repositories/`
3. Add to `__init__.py` exports
4. Create comprehensive tests

### Custom Repository Methods

```python
class CustomRepository(BaseDynamoRepository[CustomModel]):
    def custom_query_method(self, **kwargs):
        # Implementation using base repository methods
        pass
```

### Configuration Extensions

```python
# Custom configuration factory
@classmethod
def for_custom_environment(cls):
    return cls(
        # Custom configuration
    )
```

### PySpark Integration Extensions

```python
# Custom integration utilities
def custom_spark_operation(integration, **kwargs):
    with integration.pipeline_run_context(...) as run_id:
        # Custom Spark operations
        pass
```

## 🚀 Performance Considerations

### Connection Management

- Lazy initialization of AWS connections
- Connection pooling and reuse
- Configurable timeout settings

### Data Processing

- Batch operations where possible  
- Efficient DynamoDB item conversions
- Minimal memory footprint for large datasets

### Caching Strategy

- Configuration caching
- Table schema caching
- Connection pooling

## 📈 Monitoring and Observability

### Logging Strategy

```python
# Structured logging with context
logger.info(
    "Pipeline operation completed",
    extra={
        "pipeline_id": pipeline_id,
        "operation": "create",
        "duration_ms": duration,
        "table_name": table_name
    }
)
```

### Metrics Integration

- Operation timing metrics
- Error rate monitoring  
- Resource utilization tracking
- Custom metric support

## 🔮 Future Architecture Considerations

### Planned Enhancements

1. **Async Support**: AsyncIO-based repository implementations
2. **Caching Layer**: Redis/ElastiCache integration
3. **Schema Evolution**: Automatic model migration support
4. **Multi-Region**: Cross-region replication support
5. **GraphQL API**: GraphQL interface for data access

### Scalability Patterns

- Horizontal partitioning strategies
- Read replica support
- Event-driven architecture integration
- Microservice decomposition patterns

This architecture provides a solid foundation for building robust, scalable data pipeline applications while maintaining clean separation of concerns and comprehensive error handling.