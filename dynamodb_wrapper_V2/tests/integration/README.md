# Integration Tests with LocalStack

This directory contains integration tests that run against a real DynamoDB instance using LocalStack. These tests complement the unit tests by providing realistic behavior testing beyond moto mocking.

## Quick Start

```bash
# Run integration tests (automatically starts LocalStack if needed)
python scripts/run_localstack_tests.py test

# Or using pytest directly (requires LocalStack to be running)
uv run pytest tests/integration/ -v
```

## LocalStack Setup

### Prerequisites

- Docker and Docker Compose installed
- Python 3.9+ with uv package manager

### Manual LocalStack Management

```bash
# Start LocalStack container
python scripts/run_localstack_tests.py start

# Check status
python scripts/run_localstack_tests.py status

# View logs
python scripts/run_localstack_tests.py logs

# Stop LocalStack
python scripts/run_localstack_tests.py stop

# Clean up (removes volumes)
python scripts/run_localstack_tests.py clean
```

### Test Runner Commands

```bash
# Run only unit tests (fast, no LocalStack required)
python scripts/run_localstack_tests.py test-unit

# Run only integration tests (requires LocalStack)
python scripts/run_localstack_tests.py test

# Run all tests (unit + integration)
python scripts/run_localstack_tests.py test-all

# Run with verbose output
python scripts/run_localstack_tests.py test -v

# Run specific test pattern
python scripts/run_localstack_tests.py test -k "test_pipeline_config"
```

## What Integration Tests Cover

### 1. Real DynamoDB Behavior
- **Actual network calls** to LocalStack DynamoDB
- **Real GSI queries** with proper indexing behavior
- **Authentic error responses** from DynamoDB service
- **Performance characteristics** with actual latency

### 2. CQRS Operations
- **Full lifecycle testing** - create, read, update, delete operations
- **Cross-domain operations** - pipeline configs, table configs, run logs
- **Query optimization** - projections, pagination, GSI usage
- **Batch operations** - bulk inserts and updates

### 3. Data Consistency
- **Timezone compliance** - UTC storage and retrieval verification
- **Concurrent updates** - simulated race conditions
- **Transaction-style operations** - batch activation/deactivation
- **Cross-table relationships** - pipeline → table → run log linkage

### 4. Error Handling
- **Real DynamoDB errors** - ConditionalCheckFailed, ResourceNotFound
- **Network timeouts** and connection issues
- **Retry mechanisms** with actual failure scenarios

## Test Structure

### `test_localstack_integration.py`
- **TestLocalStackCQRSOperations**: Core CQRS pattern testing with real DynamoDB
- **TestLocalStackDataConsistency**: Data consistency and transaction testing

### Key Test Scenarios

1. **`test_pipeline_config_full_lifecycle`**
   - Complete CRUD operations with real storage
   - Timezone compliance verification
   - Query optimization testing

2. **`test_gsi_queries_with_real_dynamodb`**
   - ActivePipelinesIndex and EnvironmentIndex testing
   - Real pagination and filtering
   - Performance measurement

3. **`test_cross_domain_operations_with_real_storage`**
   - Pipeline → Table → Run Log relationship testing
   - Cross-domain queries and updates
   - Cascading update patterns

4. **`test_timezone_compliance_with_real_storage`**
   - Real storage/retrieval timezone cycle
   - UTC enforcement at storage layer
   - Timezone assertion helpers usage

5. **`test_error_handling_with_real_dynamodb`**
   - Authentic DynamoDB error responses
   - Conflict detection and handling
   - ItemNotFound scenarios

6. **`test_performance_characteristics_with_real_network`**
   - Batch operation efficiency
   - Projection query optimization
   - Count operation performance

## Timezone Compliance Testing

Integration tests include comprehensive timezone compliance verification:

```python
from tests.helpers.timezone_assertions import (
    assert_utc_timezone,
    assert_timezone_equals, 
    assert_timezones_equivalent,
    assert_stored_as_utc_string
)

# Verify all stored times are UTC
assert_utc_timezone(created_run.start_time)
assert_utc_timezone(created_run.end_time)

# Verify timezone equivalence across storage cycles
assert_timezones_equivalent(retrieved_run.start_time, original_start_time)
```

## Configuration

### LocalStack DynamoDB Configuration

The integration tests use LocalStack with these settings:

```yaml
# docker-compose.localstack.yml
services:
  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"
    environment:
      - SERVICES=dynamodb,s3,lambda,logs
      - DYNAMODB_ERROR_PROBABILITY=0.0
      - PERSISTENCE=1
```

### Test Configuration

```python
# Integration test configuration
localstack_config = DynamoDBConfig(
    aws_access_key_id="test",
    aws_secret_access_key="test", 
    region_name="us-east-1",
    endpoint_url="http://localhost:4566",
    environment="integration",
    table_prefix="integration_"
)
```

## CI/CD Integration

### GitHub Actions Example

```yaml
- name: Start LocalStack
  run: python scripts/run_localstack_tests.py start

- name: Run Integration Tests
  run: python scripts/run_localstack_tests.py test -v

- name: Stop LocalStack
  run: python scripts/run_localstack_tests.py stop
  if: always()
```

### Local Development Workflow

```bash
# One-time setup
uv sync

# Development cycle
python scripts/run_localstack_tests.py test-unit    # Fast feedback
python scripts/run_localstack_tests.py test        # Full integration testing
python scripts/run_localstack_tests.py clean       # Cleanup when done
```

## Troubleshooting

### LocalStack Not Starting

```bash
# Check Docker status
docker ps

# Check LocalStack logs
python scripts/run_localstack_tests.py logs

# Clean and restart
python scripts/run_localstack_tests.py clean
python scripts/run_localstack_tests.py start
```

### Test Failures

```bash
# Run with verbose output
python scripts/run_localstack_tests.py test -v

# Run specific failing test
python scripts/run_localstack_tests.py test -k "test_specific_failure"

# Check LocalStack health
curl http://localhost:4566/_localstack/health
```

### Performance Issues

- LocalStack startup can take 10-30 seconds
- Integration tests are slower than unit tests (expected)
- Use `test-unit` for fast feedback during development
- Use `test` for comprehensive validation before commits

## Benefits Over Unit Tests

### 1. **Realistic Behavior**
- Actual DynamoDB responses and error conditions
- Real GSI query behavior and limitations
- Authentic performance characteristics

### 2. **Integration Validation**
- End-to-end CQRS pattern verification
- Cross-domain operation testing
- Real network latency and timeout handling

### 3. **Confidence in Production**
- Tests run against actual DynamoDB service (via LocalStack)
- Validation of timezone compliance in real storage cycles
- Performance regression detection

### 4. **Debugging Capabilities**
- Real error messages from DynamoDB
- Network-level debugging possible
- Container logs available for troubleshooting

Both unit tests (moto) and integration tests (LocalStack) are valuable:
- **Unit tests**: Fast feedback, isolated component testing
- **Integration tests**: Realistic behavior, end-to-end validation