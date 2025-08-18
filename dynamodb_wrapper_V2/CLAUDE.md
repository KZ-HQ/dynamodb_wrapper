# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DynamoDB wrapper library built with Python that provides a **Command Query Responsibility Segregation (CQRS)** interface for DynamoDB operations using boto3 and Pydantic. The library is specifically designed for use in PySpark pipeline environments.

## Development Setup

### Virtual Environment
- **Python Version**: 3.9+
- **Virtual Environment**: `.venv/` (managed by uv)
- **Activation**: Handled automatically by `uv run`

### Dependencies
Install dependencies with:
```bash
uv sync
```

Key dependencies:
- `boto3` - AWS SDK for DynamoDB operations
- `pydantic>=2.0.0` - Data validation and serialization
- `pyspark>=3.3.0` - Spark integration utilities
- `pytest` - Testing framework
- `moto[dynamodb]` - DynamoDB mocking for tests

### Common Commands

- **Install dependencies**: `uv sync`
- **Run tests**: `uv run pytest` or `uv run pytest tests/unit/` for unit tests only
- **Run examples**: `uv run python examples/basic_usage.py`
- **Run specific test file**: `uv run pytest tests/unit/test_timezone_manager.py -v`
- **Run integration tests**: `uv run pytest tests/integration/ -v`
- **Format code**: `uv run ruff format .`
- **Lint code**: `uv run ruff check .`

## Architecture Overview

The library follows a **CQRS (Command Query Responsibility Segregation)** architecture with clear domain boundaries, featuring:

- **🎯 CQRS Pattern**: Separate optimized read/write operations with domain boundaries
- **📊 Meta Class Single Source**: Model metadata centralized in domain model Meta classes  
- **🔄 DynamoDB Compatibility**: Automatic boolean-to-string conversion for GSI keys
- **🌍 Modern Python**: Python 3.9+ native zoneinfo timezone support
- **🧪 Comprehensive Testing**: 230 total tests (218 unit + 12 integration) covering all critical scenarios and edge cases

> **📖 For complete architecture documentation, see [ARCHITECTURE.md](./ARCHITECTURE.md)**
> 
> Includes: Detailed layer breakdown, data flow diagrams, performance optimizations, extension points, SOLID principles compliance, and timezone compliance architecture.

## Timezone Compliance Architecture

The V2 architecture enforces strict timezone compliance with Python 3.9+ native support. All timezone operations follow architectural boundaries to ensure data consistency and prevent timezone-related bugs.

> **📖 For comprehensive timezone compliance rules, configuration hierarchy, and implementation details, see [ARCHITECTURE.md](./ARCHITECTURE.md#-timezone-compliance-architecture)**
>
> Covers: Gateway/Handler/DTO layer boundaries, configuration precedence, compliance rules, and DynamoDB compatibility features.

## Table Schema

The library manages three DynamoDB tables:

1. **pipeline_config** - Pipeline configurations
   - Primary Key: `pipeline_id` (String)

2. **table_config** - Table metadata and processing configuration  
   - Primary Key: `table_id` (String)

3. **pipeline_run_logs** - Pipeline execution logs
   - Primary Key: `run_id` (String)
   - Sort Key: `pipeline_id` (String) - Composite key for run logs

## Testing Strategy

- **Unit Tests** (`tests/unit/`): Test individual components with mocking
- **Integration Tests** (`tests/integration/`): Test with actual DynamoDB (local or AWS)
- **Timezone Tests** (`tests/unit/test_timezone_manager.py`): Comprehensive timezone compliance tests
- **Model-Driven Tests** (`tests/unit/test_model_driven_keys.py`): Meta class and key building tests
- **Integration Tests** (`tests/integration/`): End-to-end CQRS and timezone compliance tests
- **Test Configuration** (`tests/conftest.py`): Shared fixtures and setup
- **Mocking**: Uses `moto` library for DynamoDB mocking in unit tests

## Usage Patterns

### CQRS API Implementation Patterns
```python
# Handler Layer Implementation Pattern (AI assistant reference)
from dynamodb_wrapper.core import create_table_gateway

# Read operations - focus on projection efficiency
class PipelineConfigReadApi:
    def __init__(self, config: DynamoDBConfig):
        self.gateway = create_table_gateway(config, "pipeline_config")
        self.config = config  # For timezone conversion
    
    def get_by_id(self, pipeline_id: str, projection: Optional[List[str]] = None):
        # Use gateway for raw DynamoDB operations
        response = self.gateway.get_item(Key={'pipeline_id': pipeline_id})
        # Convert to view model with timezone handling
        return self._convert_to_view_model(response.get('Item'))

# Write operations - focus on validation and consistency  
class PipelineConfigWriteApi:
    def create_pipeline(self, pipeline_data: PipelineConfigUpsert):
        # DTO validation happens here, convert timezone to UTC
        validated_data = self._convert_timezone_to_utc(pipeline_data)
        # Use gateway for storage
        return self.gateway.put_item(Item=validated_data)
```

### Timezone Configuration and Handling

#### Configuration Options:

```python
# Environment-based configuration
# Note: Internal operations always use UTC (hardcoded for consistency)
export DYNAMODB_USER_TIMEZONE="America/New_York"  # User display timezone only

# Programmatic configuration
config = DynamoDBConfig(
    user_timezone="America/New_York"     # Only user timezone is configurable
    # Internal operations always use UTC (not configurable)
)
```

#### Timezone Precedence (High to Low):

1. **Function-level parameters** (highest) - explicit timezone in utility functions
2. **Configuration `user_timezone`** - automatic conversion in handler layer
3. **No conversion** (returns UTC) - internal operations always use UTC

#### Usage Patterns:

```python
from dynamodb_wrapper.utils import to_utc, ensure_timezone_aware, to_user_timezone

# Handler layer converts user input to UTC before storage
user_time = datetime.now(user_timezone)
utc_time = to_utc(user_time)  # Convert at handler boundary

# Store in UTC (gateway layer receives UTC-only)
run_log = PipelineRunLogUpsert(
    run_id="run-123",
    start_time=utc_time  # Always UTC
)

# Configuration-based automatic conversion
config = DynamoDBConfig(user_timezone="America/New_York")
read_api = PipelineRunLogsReadApi(config)
run_log = read_api.get_by_id("run-123")  # Times auto-converted to ET

# Function-level timezone override (highest precedence)
utc_time = datetime.now(timezone.utc)
west_coast_time = to_user_timezone(utc_time, "America/Los_Angeles")
london_time = to_user_timezone(utc_time, "Europe/London")
```

### Model-Driven Implementation Details (AI Assistant Reference)
```python
# Meta Class Pattern Implementation - Key utilities usage
from dynamodb_wrapper.utils import (
    extract_model_metadata,
    build_model_key,
    build_gsi_key_condition
)

# When implementing new handlers, use these patterns:
def _build_query_key(model_class, **key_values):
    """Extract metadata from Meta class and build DynamoDB key."""
    metadata = extract_model_metadata(model_class)
    # Use metadata['partition_key'], metadata['sort_key'] for type-safe operations
    return build_model_key(model_class, **key_values)

def _convert_for_storage(model_instance):
    """Convert model to DynamoDB item with boolean-to-string conversion."""
    # This handles boolean → 'true'/'false' for GSI compatibility
    return model_instance.to_dynamodb_item()

def _convert_from_storage(item_dict, model_class):
    """Convert DynamoDB item back to model with string-to-boolean conversion."""
    # This handles 'true'/'false' → boolean for Python compatibility  
    return model_class.from_dynamodb_item(item_dict)
```

## 🎯 Rule of Thumb (Quick Decision Guide)

### When to Use What

**🔍 Read Operations (Queries)**
- Use `*ReadApi` classes for all data retrieval
- Always specify `projection` for better performance
- Use GSI queries for non-primary-key lookups
- Apply `limit` and pagination for large datasets

**✏️ Write Operations (Commands)**  
- Use `*WriteApi` classes for all data modifications
- Always use DTO models (`*Upsert`) for input validation
- Use conditional expressions to prevent race conditions
- Batch operations for multiple items (auto-chunking at 25 items)

**🕐 Timezone Handling**
- **Storage**: Always UTC (gateway layer enforces this)
- **Display**: Use `config.user_timezone` for automatic conversion
- **Override**: Use `to_user_timezone(dt, "America/New_York")` for specific timezone

**🔧 Key Building**
- Use `build_model_key()` with model class + key values
- Use `build_gsi_key_condition()` for GSI queries
- Meta class is single source of truth - never hardcode keys

**🚨 Error Handling**
- Catch domain-specific exceptions (`ItemNotFoundError`, `ConflictError`)
- Use `RetryableError` for operations that can be retried
- Check error context for debugging information

### Quick Architecture Rules

1. **Meta Class First**: If model metadata is needed, check the Meta class
2. **UTC Only Storage**: Gateway layer operations are always UTC
3. **Handler Boundaries**: Timezone conversion happens at handler layer only  
4. **Boolean GSI**: Booleans auto-convert to 'true'/'false' strings for GSI compatibility
5. **View Models**: Use for read projections, never for writes
6. **DTO Models**: Use for write validation, never for storage

## Development Notes

- All models use Pydantic v2 for validation
- CQRS APIs follow consistent naming conventions (ReadApi/WriteApi)
- Error handling uses domain-specific exceptions
- Configuration supports multiple environments (dev, staging, prod)
- PySpark utilities handle Spark session configuration automatically
- Context managers ensure proper resource cleanup and error logging
- Timezone compliance is enforced through architectural boundaries

## Common Tasks

### Adding New Domain Operations

1. **Read Operation**: Add to appropriate `queries.py` file
2. **Write Operation**: Add to appropriate `commands.py` file  
3. **New Domain**: Create new domain directory with `queries.py` and `commands.py`
4. **Timezone Handling**: Use `config.user_timezone` for automatic conversion or `to_user_timezone()` for explicit timezone control

### Timezone Implementation Guidelines

1. **Internal Operations**: Always use UTC (hardcoded for consistency)
   - Gateway layer operations are always UTC
   - Storage operations always use UTC
   - Data processing always assumes UTC
2. **Handler Layer**: Use `self.config.user_timezone` for display conversion only
3. **Function Level**: Use `to_user_timezone(dt, timezone)` for explicit timezone override
4. **Configuration**: Only `user_timezone` is configurable (internal ops are always UTC)

### Testing New Features

1. Add unit tests with mocking
2. Add integration tests if needed
3. **Test timezone compliance** with different `user_timezone` configurations
4. Verify function-level timezone overrides work correctly
5. Run full test suite: `uv run pytest`

### Code Quality Checks

```bash
uv run ruff format .  # Format
uv run ruff check .   # Lint
uv run mypy dynamodb_wrapper  # Type check
```

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.

      
      IMPORTANT: this context may or may not be relevant to your tasks. You should not respond to this context unless it is highly relevant to your task.