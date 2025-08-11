# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DynamoDB wrapper library built with Python that provides a repository pattern interface for DynamoDB operations using boto3 and Pydantic. The library is specifically designed for use in PySpark pipeline environments.

## Development Setup

### Virtual Environment
- **Python Version**: 3.9
- **Virtual Environment**: `venv/` (already created)
- **Activation**: `source venv/bin/activate` (macOS/Linux) or `venv\Scripts\activate` (Windows)

### Dependencies
Install dependencies with:
```bash
pip install -r requirements.txt
```

Key dependencies:
- `boto3` - AWS SDK for DynamoDB operations
- `pydantic>=2.0.0` - Data validation and serialization
- `pyspark>=3.3.0` - Spark integration utilities
- `pytest` - Testing framework
- `moto[dynamodb]` - DynamoDB mocking for tests

### Common Commands

- **Install dependencies**: `pip install -r requirements.txt`
- **Run tests**: `pytest` or `pytest tests/unit/` for unit tests only
- **Run examples**: `python examples/basic_usage.py` or `python examples/pyspark_usage.py`
- **Run specific test file**: `pytest tests/unit/test_models.py -v`

## Architecture Overview

The library follows a clean architecture pattern with these key components:

### Core Components
- **Models** (`dynamodb_wrapper/models/`): Pydantic models for data validation
  - `PipelineConfig` - Pipeline configuration and metadata
  - `TableConfig` - Table schema and processing configuration
  - `PipelineRunLog` - Pipeline execution logs and metrics

- **Repositories** (`dynamodb_wrapper/repositories/`): Repository pattern implementations
  - `BaseDynamoRepository` - Abstract base class with common CRUD operations
  - `PipelineConfigRepository` - Pipeline configuration management
  - `TableConfigRepository` - Table metadata management
  - `PipelineRunLogsRepository` - Run log and monitoring data

- **Configuration** (`dynamodb_wrapper/config/`): Configuration management
  - `DynamoDBConfig` - Centralized configuration with environment variable support

- **Utils** (`dynamodb_wrapper/utils/`): PySpark integration utilities
  - `SparkDynamoDBIntegration` - Main integration class for Spark environments
  - Helper functions for Spark session creation and data processing

- **Exceptions** (`dynamodb_wrapper/exceptions/`): Custom exception hierarchy
  - `DynamoDBWrapperError` - Base exception
  - `ItemNotFoundError` - Item not found errors
  - `ValidationError` - Data validation errors
  - `ConnectionError` - Connection issues

### Key Design Patterns
- **Repository Pattern**: Abstracts DynamoDB operations behind clean interfaces
- **Generic Types**: Type-safe repository implementations using Python generics
- **Context Managers**: Automatic pipeline run logging with proper error handling
- **Configuration Classes**: Centralized configuration with environment support

## Table Schema

The library manages three DynamoDB tables:

1. **pipeline_config** - Stores pipeline configurations
   - Primary Key: `pipeline_id` (String)

2. **table_config** - Stores table metadata and processing configuration  
   - Primary Key: `table_id` (String)

3. **pipeline_run_logs** - Stores pipeline execution logs
   - Primary Key: `run_id` (String)

## Testing Strategy

- **Unit Tests** (`tests/unit/`): Test individual components with mocking
- **Integration Tests** (`tests/integration/`): Test with actual DynamoDB (local or AWS)
- **Test Configuration** (`tests/conftest.py`): Shared fixtures and setup
- **Mocking**: Uses `moto` library for DynamoDB mocking in unit tests

## Usage Patterns

### Basic Repository Usage
```python
config = DynamoDBConfig.from_env()
pipeline_repo = PipelineConfigRepository(config)
pipeline = pipeline_repo.get_by_pipeline_id("my-pipeline")
```

### PySpark Integration
```python
integration = SparkDynamoDBIntegration(config)
with integration.pipeline_run_context("pipeline-id") as run_id:
    spark = integration.create_spark_session("pipeline-id")
    # Process data
```

## Development Notes

- All models use Pydantic v2 for validation
- Repository methods follow consistent naming conventions
- Error handling is comprehensive with custom exception types
- Configuration supports multiple environments (dev, staging, prod)
- PySpark utilities handle Spark session configuration automatically
- Context managers ensure proper resource cleanup and error logging