# 🏗️ DynamoDB Wrapper Monorepo

A monolithic repository containing multiple versions of the DynamoDB wrapper library.

## 📁 Repository Structure

### 🔹 dynamodb_wrapper_V1/
The first version of the DynamoDB wrapper library - a comprehensive Python library that provides a repository pattern wrapper around AWS DynamoDB using boto3 and Pydantic, specifically designed for PySpark pipeline environments.

**Status**: ✅ Stable and Production Ready  
**Documentation**: [V1 README](./dynamodb_wrapper_V1/README_V1.md)  
**Architecture**: [V1 Architecture](./dynamodb_wrapper_V1/ARCHITECTURE_V1.md)  

### 🔹 dynamodb_wrapper_V2/ (Coming Soon)
The next generation of the DynamoDB wrapper library with enhanced features and improvements.

**Status**: 🚧 Under Development

## 🚀 Getting Started

### Using V1
```bash
cd dynamodb_wrapper_V1
uv sync
uv run pytest tests/unit/
```

### Package Installation (V1)
```bash
cd dynamodb_wrapper_V1
pip install -e .
```

Then import in your code:
```python
from dynamodb_wrapper_V1.dynamodb_wrapper import (
    DynamoDBConfig,
    PipelineConfigRepository,
    TableConfigRepository,
    PipelineRunLogsRepository
)
```

## 🔄 Version Comparison

| Feature | V1 | V2 |
|---------|----|----|
| Repository Pattern | ✅ | 🚧 |
| Pydantic Models | ✅ | 🚧 |
| PySpark Integration | ✅ | 🚧 |
| Timezone Support | ✅ | 🚧 |
| Testing Coverage | ✅ 94 tests | 🚧 |

## 🤝 Contributing

Each version has its own development workflow:

### V1 Development
```bash
cd dynamodb_wrapper_V1
uv sync
# Make changes
uv run pytest tests/unit/
uv run ruff check .
```

### V2 Development (Coming Soon)
```bash
cd dynamodb_wrapper_V2
# Development workflow TBD
```

## 📄 License

MIT License - see LICENSE file for details.

## 📝 Changelog

### Monorepo Restructure
- **2025-08-11**: Restructured as monorepo with V1 in separate directory
- **2025-08-11**: Fixed all import paths for V1 structure
- **2025-08-11**: Prepared foundation for V2 development

### V1 History
- **Version 1.0.1**: Complete DynamoDB wrapper with timezone support, 94 unit tests, comprehensive documentation