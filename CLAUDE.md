# CLAUDE.md - Monorepo AI Assistant Guidance

This file provides guidance to Claude Code (claude.ai/code) when working with this DynamoDB wrapper monorepo.

## 🎯 Quick Decision Guide

### **Version Selection**
- **For new development**: Use V2 (CQRS architecture) - production ready with 222/222 tests
- **For legacy maintenance**: Use V1 (Repository pattern) - stable with 94 tests
- **When uncertain**: Default to V2 unless explicitly working with V1 code

### **Directory Navigation**
```
/Users/keith/PycharmProjects/pythonProject/
├── dynamodb_wrapper_V1/     # Legacy repository pattern
└── dynamodb_wrapper_V2/     # Current CQRS architecture (recommended)
```

## 📁 Project Structure Overview

### **V1: Repository Pattern (Legacy)**
**Location**: `./dynamodb_wrapper_V1/`  
**Pattern**: Traditional repository pattern with generic base classes  
**Documentation**: [V1 CLAUDE.md](./dynamodb_wrapper_V1/CLAUDE_V1.md)  
**Status**: Maintenance mode - critical fixes only

**Key Files:**
- `repositories/base.py` - Generic repository implementation
- `models/` - Simple Pydantic models
- `tests/unit/` - 94 unit tests

### **V2: CQRS Architecture (Current) ⭐ RECOMMENDED**
**Location**: `./dynamodb_wrapper_V2/`  
**Pattern**: CQRS with domain-driven design and Meta class single source of truth  
**Documentation**: [V2 CLAUDE.md](./dynamodb_wrapper_V2/CLAUDE.md) with Rule of Thumb guide  
**Status**: Production ready - active development

**Key Files:**
- `handlers/` - CQRS Read/Write APIs by domain
- `models/domain_models.py` - Meta class single source of truth
- `models/views.py` - Read-optimized models
- `models/dtos.py` - Write-optimized models  
- `tests/` - 222 comprehensive tests (218 unit + 4 integration)

## 🛠️ Development Workflows

### **Working with V2 (Recommended)**
```bash
cd dynamodb_wrapper_V2
uv sync
# Development commands
uv run pytest                    # 222 tests
uv run ruff format . && uv run ruff check .
```

**Architecture Patterns:**
- Use `*ReadApi` classes for all data retrieval with projections
- Use `*WriteApi` classes for all data modifications with DTO validation
- Meta class defines all keys/GSIs - never hardcode metadata
- UTC-only storage, timezone conversion at handler boundaries

**Quick Reference:**
- Comprehensive guidance: [V2 CLAUDE.md](./dynamodb_wrapper_V2/CLAUDE.md)
- Rule of Thumb decisions: [V2 CLAUDE.md#rule-of-thumb](./dynamodb_wrapper_V2/CLAUDE.md#-rule-of-thumb-quick-decision-guide)

### **Working with V1 (Legacy)**
```bash
cd dynamodb_wrapper_V1  
uv sync
# Development commands
uv run pytest tests/unit/       # 94 tests
uv run ruff check .
```

**Architecture Patterns:**
- Use repository classes for all database operations
- Generic CRUD operations with manual key construction
- Standard Pydantic model validation

**Reference:**
- Detailed guidance: [V1 CLAUDE.md](./dynamodb_wrapper_V1/CLAUDE_V1.md)

## 🎯 Context-Aware Development

### **When to Use V1**
- Explicitly working in `dynamodb_wrapper_V1/` directory
- Maintaining existing V1 codebase
- User specifically requests V1 functionality
- Critical bug fixes for legacy systems

### **When to Use V2** (Default)
- Starting new development work
- User asks about "the latest version" or "current implementation"
- Performance optimization requirements
- Need comprehensive testing or advanced DynamoDB features
- Working in `dynamodb_wrapper_V2/` directory or unspecified location

## 🧭 Navigation Rules

### **Auto-Detection:**
1. **Current working directory** - Check if in V1 or V2 subdirectory
2. **File references** - Files ending in `_V1.md` are V1, others are V2
3. **Import patterns** - `from dynamodb_wrapper_V1` indicates V1 context
4. **User intent** - "repository pattern" suggests V1, "CQRS" suggests V2

### **Default Behavior:**
- **When unclear**: Default to V2 (modern, production-ready)
- **Documentation requests**: Provide version-specific links
- **Architecture questions**: Compare both versions with V2 recommendation

## 📚 Documentation Hierarchy

### **Monorepo Level** (Current Location)
- `README.md` - Version comparison and quick start
- `ARCHITECTURE.md` - High-level architecture comparison
- `CLAUDE.md` - This file - monorepo AI guidance

### **V1 Specific** (`./dynamodb_wrapper_V1/`)
- `README_V1.md` - V1 usage and examples
- `ARCHITECTURE_V1.md` - Repository pattern details
- `CLAUDE_V1.md` - V1 development guidance

### **V2 Specific** (`./dynamodb_wrapper_V2/`)
- `README.md` - V2 usage and examples (user-focused)
- `ARCHITECTURE.md` - CQRS architecture details with TL;DR (technical)
- `CLAUDE.md` - V2 development guidance with Rule of Thumb (AI assistant)

## ⚡ Quick Actions

### **Common Tasks:**
- **Run tests**: `cd dynamodb_wrapper_V2 && uv run pytest` (V2) or `cd dynamodb_wrapper_V1 && uv run pytest tests/unit/` (V1)
- **Check imports**: V2 is production-ready, V1 for maintenance
- **Architecture questions**: Reference version-specific architecture docs
- **Performance needs**: Recommend V2 (50-80% payload reduction)
- **Type safety**: Recommend V2 (Meta class single source of truth)

### **File Operations:**
- **Read code**: Check directory context to determine version
- **Write code**: Default to V2 unless explicitly in V1 context
- **Documentation**: Use version-appropriate examples and patterns

## 🔄 Version-Specific Features

### **V1 Capabilities**
- Repository pattern with generic base classes
- Basic DynamoDB operations
- Pydantic v1 compatibility
- 94 unit tests
- Python 3.8+ support

### **V2 Advantages** 
- CQRS with separate read/write optimization
- Meta class single source of truth
- 50-80% performance improvement
- 222 comprehensive tests (136% more)
- Modern Python 3.9+ with native zoneinfo
- Advanced DynamoDB features (boolean conversion, flexible views)

## 🎨 Best Practices

### **Code Style:**
- Follow version-specific patterns and conventions
- Use version-appropriate documentation references
- Maintain consistency within chosen version
- Follow testing patterns established in each version

### **Documentation:**
- Reference version-specific docs for detailed guidance
- Use architecture-appropriate examples
- Highlight version differences when relevant

### **Decision Making:**
- When in doubt, choose V2 for new work
- Respect user's explicit version choice
- Provide migration guidance when beneficial

---

**For detailed version-specific guidance:**
- **V2 (Recommended)**: [V2 CLAUDE.md](./dynamodb_wrapper_V2/CLAUDE.md)
- **V1 (Legacy)**: [V1 CLAUDE.md](./dynamodb_wrapper_V1/CLAUDE_V1.md)