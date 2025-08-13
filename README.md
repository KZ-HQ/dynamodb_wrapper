# 🏗️ DynamoDB Wrapper Monorepo

A monolithic repository containing multiple versions of the DynamoDB wrapper library, showcasing the evolution from repository pattern to modern CQRS architecture.

## 📁 Repository Structure

### 🔹 dynamodb_wrapper_V1/ - Repository Pattern (Legacy)
The original version implementing a clean repository pattern wrapper around AWS DynamoDB using boto3 and Pydantic, designed for PySpark pipeline environments.

**Status**: ✅ Stable and Production Ready  
**Pattern**: Repository Pattern with Generic Base Classes  
**Documentation**: [V1 README](./dynamodb_wrapper_V1/README_V1.md) | [V1 Architecture](./dynamodb_wrapper_V1/ARCHITECTURE_V1.md)

### 🔹 dynamodb_wrapper_V2/ - CQRS Architecture (Current)
The next generation implementing **Command Query Responsibility Segregation (CQRS)** with Meta class single source of truth, modern Python 3.9+ features, and comprehensive testing.

**Status**: ✅ **Production Ready**  
**Pattern**: CQRS with Domain-Driven Design  
**Testing**: 222/222 tests passing (218 unit + 4 integration)  
**Documentation**: [V2 README](./dynamodb_wrapper_V2/README.md) | [V2 Architecture](./dynamodb_wrapper_V2/ARCHITECTURE.md)  
**Version**: 1.0.0

## 🎯 Quick Start Guide

### **Recommended: Use V2 (CQRS Architecture)**
```bash
cd dynamodb_wrapper_V2
uv sync
uv run pytest  # 222/222 tests passing
```

### Legacy: Use V1 (Repository Pattern)  
```bash
cd dynamodb_wrapper_V1
uv sync
uv run pytest tests/unit/
```

## 🔄 Architecture Comparison

| Aspect | V1 - Repository Pattern | V2 - CQRS Architecture |
|--------|------------------------|-------------------------|
| **Pattern** | Repository with Generic Base | CQRS with Domain-Driven Design |
| **Models** | Pydantic v1 Compatible | Pydantic v2 with Meta Classes |
| **Operations** | Unified Repository Interface | Separate Read/Write APIs |
| **Key Management** | Manual Key Construction | Meta Class Single Source of Truth |
| **Testing** | 94 unit tests | **222 tests** (218 unit + 4 integration) |
| **Python Support** | 3.8+ | **3.9+ (Native zoneinfo)** |
| **Performance** | Standard | **50-80% payload reduction** |
| **DynamoDB Compat** | Basic | **Advanced** (boolean conversion, flexible views) |
| **Documentation** | Good | **Comprehensive** (TL;DR, Rule of Thumb) |

### **V2 Advantages**
- 🎯 **CQRS Optimization**: Separate read/write APIs optimized for different usage patterns
- 📊 **Meta Class Pattern**: Single source of truth eliminates metadata inconsistencies  
- 🔄 **DynamoDB Compatibility**: Automatic boolean-to-string conversion for GSI keys
- 🧪 **Comprehensive Testing**: 136% more tests with edge case coverage
- ⚡ **Performance**: Flexible view models reduce payload sizes by 50-80%

## 🤝 Contributing

### **Recommended: Contribute to V2**
V2 is the active development branch with modern architecture:
```bash
cd dynamodb_wrapper_V2
uv sync
# Make changes
uv run pytest  # 222 tests
uv run ruff format . && uv run ruff check .
```

### Legacy: V1 Maintenance
V1 is in maintenance mode for critical fixes only:
```bash
cd dynamodb_wrapper_V1
uv sync
uv run pytest tests/unit/
uv run ruff check .
```

## 📄 License

MIT License - see LICENSE file for details.

## 📝 Recent Updates

### **2025-08-13: V2 Production Release (1.0.0)**
- ✅ **Complete CQRS Architecture** with Meta class single source of truth
- ✅ **222/222 tests passing** with comprehensive edge case coverage  
- ✅ **Production-ready documentation** with TL;DR and Rule of Thumb guides
- ✅ **Modern Python 3.9+** with native zoneinfo timezone support

### 2025-08-11: Monorepo Setup
- Restructured as monorepo with V1 isolated in separate directory
- Fixed V1 import paths for independent operation
- Established foundation for V2 development

### V1 Legacy (1.0.1)
- Repository pattern DynamoDB wrapper with 94 unit tests
- Comprehensive timezone support and PySpark integration