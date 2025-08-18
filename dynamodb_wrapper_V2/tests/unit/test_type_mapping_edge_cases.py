"""
Tests for Pydantic-DynamoDB type mapping edge cases.

This module tests decimal precision, Optional/Union types, and other edge cases
in the type mapping between Pydantic models and DynamoDB items identified
in TEST_ANALYSIS.md.
"""

import pytest
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from typing import Optional, Union
from unittest.mock import patch

from dynamodb_wrapper.models.domain_models import PipelineConfig, PipelineRunLog, TableConfig
from dynamodb_wrapper.exceptions import ValidationError
from pydantic import BaseModel, Field, ValidationError as PydanticValidationError


class TestDecimalPrecisionHandling:
    """Test decimal precision preservation and edge cases."""

    def test_high_precision_decimal_preservation(self):
        """Test that high-precision decimal values are preserved exactly."""
        # Test various precision levels
        test_cases = [
            Decimal("15.75"),           # Simple decimal
            Decimal("15.123456789"),    # High precision
            Decimal("0.000000001"),     # Very small
            Decimal("999999999.999"),   # Large with decimals
            Decimal("123.0"),           # Integer-like decimal
            Decimal("0"),               # Zero
            Decimal("-15.75"),          # Negative
        ]
        
        for test_decimal in test_cases:
            pipeline = PipelineConfig(
                pipeline_id="decimal-test",
                pipeline_name="Decimal Test",
                source_type="s3",
                destination_type="warehouse",
                memory_gb=test_decimal
            )
            
            # Convert to DynamoDB item and back
            item = pipeline.to_dynamodb_item()
            restored = PipelineConfig.from_dynamodb_item(item)
            
            # Should preserve exact decimal value
            assert isinstance(restored.memory_gb, Decimal)
            assert restored.memory_gb == test_decimal
            assert str(restored.memory_gb) == str(test_decimal)

    def test_decimal_serialization_in_dynamodb_item(self):
        """Test how decimals are serialized in DynamoDB items."""
        pipeline = PipelineConfig(
            pipeline_id="decimal-serialization",
            pipeline_name="Decimal Serialization Test",
            source_type="s3",
            destination_type="warehouse",
            memory_gb=Decimal("15.123456789")
        )
        
        item = pipeline.to_dynamodb_item()
        
        # In DynamoDB, decimal should remain as Decimal (boto3 handles this)
        assert isinstance(item['memory_gb'], Decimal)
        assert item['memory_gb'] == Decimal("15.123456789")

    def test_decimal_edge_cases(self):
        """Test decimal edge cases that might cause issues."""
        edge_cases = [
            (Decimal("0.1") + Decimal("0.2"), "floating point precision"),
            (Decimal("1E+2"), "scientific notation"),
            (Decimal("1E-10"), "very small scientific"),
            (Decimal("inf"), "positive infinity"),
            (Decimal("-inf"), "negative infinity"),
        ]
        
        for test_decimal, description in edge_cases:
            try:
                pipeline = PipelineConfig(
                    pipeline_id="edge-case-test",
                    pipeline_name="Edge Case Test",
                    source_type="s3",
                    destination_type="warehouse",
                    memory_gb=test_decimal
                )
                
                # Should handle conversion properly
                item = pipeline.to_dynamodb_item()
                restored = PipelineConfig.from_dynamodb_item(item)
                
                if test_decimal.is_finite():
                    assert restored.memory_gb == test_decimal
                else:
                    # Infinity values should be handled gracefully
                    assert not restored.memory_gb.is_finite()
                    
            except (ValidationError, PydanticValidationError, InvalidOperation):
                # Some edge cases may be rejected by validation, which is acceptable
                print(f"Edge case rejected as expected: {description}")

    def test_decimal_from_string_in_dynamodb(self):
        """Test decimal restoration from string values in DynamoDB."""
        # Simulate DynamoDB item with decimal as string (edge case)
        item = {
            'pipeline_id': 'string-decimal-test',
            'pipeline_name': 'String Decimal Test',
            'source_type': 's3',
            'destination_type': 'warehouse',
            'memory_gb': '15.123456789',  # String instead of Decimal
            'is_active': 'true',
            'environment': 'dev',
            'version': '1.0.0',
            'created_at': '2024-01-01T10:00:00+00:00',
            'updated_at': '2024-01-01T10:00:00+00:00'
        }
        
        # Should convert string to Decimal properly
        restored = PipelineConfig.from_dynamodb_item(item)
        assert isinstance(restored.memory_gb, (Decimal, float))
        assert float(restored.memory_gb) == 15.123456789

    def test_decimal_none_value(self):
        """Test handling of None for optional decimal fields."""
        pipeline = PipelineConfig(
            pipeline_id="none-decimal-test",
            pipeline_name="None Decimal Test",
            source_type="s3",
            destination_type="warehouse",
            memory_gb=None  # None value for optional field
        )
        
        item = pipeline.to_dynamodb_item()
        
        # None values should not appear in DynamoDB item
        assert 'memory_gb' not in item
        
        restored = PipelineConfig.from_dynamodb_item(item)
        assert restored.memory_gb is None


class TestOptionalAndUnionTypeHandling:
    """Test Optional and Union type handling edge cases."""

    def test_optional_string_fields(self):
        """Test Optional[str] fields with various values."""
        test_values = [
            None,
            "",  # Empty string (should be validated by Pydantic)
            "normal string",
            "string with special chars: !@#$%^&*()",
            "very " + "long " * 100 + "string",  # Very long string
        ]
        
        for test_value in test_values:
            try:
                pipeline = PipelineConfig(
                    pipeline_id="optional-string-test",
                    pipeline_name="Optional String Test",
                    source_type="s3",
                    destination_type="warehouse",
                    description=test_value  # Optional[str] field
                )
                
                item = pipeline.to_dynamodb_item()
                restored = PipelineConfig.from_dynamodb_item(item)
                
                if test_value is None:
                    assert 'description' not in item
                    assert restored.description is None
                else:
                    assert item['description'] == test_value
                    assert restored.description == test_value
                    
            except PydanticValidationError:
                # Some values (like empty string) may be rejected by validation
                print(f"Value rejected by validation: {repr(test_value)}")

    def test_optional_integer_fields(self):
        """Test Optional[int] fields."""
        test_values = [
            None,
            0,
            1,
            -1,
            2**31 - 1,  # Max 32-bit int
            2**63 - 1,  # Max 64-bit int
        ]
        
        for test_value in test_values:
            pipeline = PipelineConfig(
                pipeline_id="optional-int-test",
                pipeline_name="Optional Int Test",
                source_type="s3",
                destination_type="warehouse",
                cpu_cores=test_value  # Optional[int] field
            )
            
            item = pipeline.to_dynamodb_item()
            restored = PipelineConfig.from_dynamodb_item(item)
            
            if test_value is None:
                assert 'cpu_cores' not in item
                assert restored.cpu_cores is None
            else:
                assert item['cpu_cores'] == test_value
                assert restored.cpu_cores == test_value

    def test_optional_dict_fields(self):
        """Test Optional[Dict] fields with complex nested data."""
        test_configs = [
            {},  # Empty dict (default)
            {"simple": "value"},
            {
                "complex": {
                    "nested": {
                        "deeply": ["list", "of", "values"]
                    }
                }
            },
            {
                "spark.sql.adaptive.enabled": True,
                "spark.sql.adaptive.coalescePartitions.enabled": True,
                "spark.executor.memory": "4g",
                "spark.executor.cores": "2"
            }
        ]
        
        for test_config in test_configs:
            pipeline = PipelineConfig(
                pipeline_id="optional-dict-test",
                pipeline_name="Optional Dict Test",
                source_type="s3",
                destination_type="warehouse",
                spark_config=test_config  # Dict field with default_factory
            )
            
            item = pipeline.to_dynamodb_item()
            restored = PipelineConfig.from_dynamodb_item(item)
            
            if test_config == {}:
                # Empty dict may or may not be in the item depending on serialization
                assert restored.spark_config == test_config
            else:
                # For the boolean test case, we need to account for boolean->string conversion
                if "spark.sql.adaptive.enabled" in test_config:
                    # Boolean values get converted to strings in DynamoDB serialization
                    expected_item_config = {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.sql.adaptive.coalescePartitions.enabled": "true",
                        "spark.executor.memory": "4g",
                        "spark.executor.cores": "2"
                    }
                    assert item['spark_config'] == expected_item_config
                    # But they should be converted back to booleans when restored
                    expected_restored_config = {
                        "spark.sql.adaptive.enabled": True,
                        "spark.sql.adaptive.coalescePartitions.enabled": True,
                        "spark.executor.memory": "4g",
                        "spark.executor.cores": "2"
                    }
                    assert restored.spark_config == expected_restored_config
                else:
                    # For non-boolean configs, they should match exactly
                    assert item['spark_config'] == test_config
                    assert restored.spark_config == test_config

    def test_optional_list_fields(self):
        """Test List fields with default factory."""
        test_lists = [
            [],  # Empty list (default)
            ["single"],
            ["multiple", "items", "here"],
            ["col1", "col2", "col3"],  # String list for partition columns
        ]
        
        for test_list in test_lists:
            table = TableConfig(
                table_id="optional-list-test",
                pipeline_id="test-pipeline",
                table_name="test_table",
                table_type="source",
                data_format="parquet",
                location="s3://bucket/path/",
                partition_columns=test_list  # List[str] field with default_factory
            )
            
            item = table.to_dynamodb_item()
            restored = TableConfig.from_dynamodb_item(item)
            
            if test_list == []:
                # Empty list may or may not be in the item depending on serialization
                assert restored.partition_columns == test_list
            else:
                assert item['partition_columns'] == test_list
                assert restored.partition_columns == test_list

    def test_union_type_fields(self):
        """Test Union type fields (if any exist in the models)."""
        # Test datetime field which could be datetime or string
        test_times = [
            datetime.now(timezone.utc),
            datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        ]
        
        for test_time in test_times:
            log = PipelineRunLog(
                run_id="union-test",
                pipeline_id="test-pipeline",
                status="pending",
                trigger_type="manual",
                start_time=test_time
            )
            
            item = log.to_dynamodb_item()
            restored = PipelineRunLog.from_dynamodb_item(item)
            
            # Datetime should be preserved or converted consistently
            assert isinstance(restored.start_time, datetime)
            assert restored.start_time.tzinfo is not None  # Should have timezone


class TestNestedModelSerialization:
    """Test serialization of nested models and complex structures."""

    def test_nested_dict_preservation(self):
        """Test that nested dictionaries are preserved exactly."""
        complex_config = {
            "level1": {
                "level2": {
                    "level3": {
                        "string_value": "test",
                        "numeric_value": 42,
                        "boolean_value": True,
                        "list_value": [1, 2, 3],
                        "null_value": None
                    }
                }
            },
            "top_level_list": [
                {"item": 1},
                {"item": 2}
            ]
        }
        
        pipeline = PipelineConfig(
            pipeline_id="nested-test",
            pipeline_name="Nested Test",
            source_type="s3",
            destination_type="warehouse",
            spark_config=complex_config
        )
        
        item = pipeline.to_dynamodb_item()
        restored = PipelineConfig.from_dynamodb_item(item)
        
        assert restored.spark_config == complex_config

    def test_deeply_nested_boolean_conversion(self):
        """Test that boolean conversion works recursively in deeply nested structures."""
        # Simulate a DynamoDB item with deeply nested boolean strings
        # This tests the recursive conversion in item_to_model after our recent changes
        dynamodb_item = {
            'pipeline_id': 'deep-nested-test',
            'pipeline_name': 'Deep Nested Boolean Test',
            'source_type': 's3',
            'destination_type': 'warehouse',
            'spark_config': {
                'level1': {
                    'feature_enabled': 'true',  # Boolean as string (DynamoDB format)
                    'level2': {
                        'nested_feature': 'false',  # Boolean as string (DynamoDB format)
                        'level3': {
                            'deep_feature': 'true',  # Boolean as string (DynamoDB format)
                            'settings': {
                                'auto_optimize': 'false',  # Boolean as string (DynamoDB format)
                                'list_with_booleans': [
                                    {'enabled': 'true'},   # Boolean in list item
                                    {'enabled': 'false'},  # Boolean in list item
                                    {'name': 'test', 'active': 'true'}  # Mixed types
                                ]
                            }
                        }
                    }
                },
                'top_level_boolean': 'true',  # Boolean as string (DynamoDB format)
                'regular_string': 'not_a_boolean',  # Should remain as string
                'datetime_string': '2024-01-01T10:00:00Z'  # Should remain as string for DateTimeMixin
            },
            'is_active': 'true',  # Top-level boolean as string
            'environment': 'dev',
            'version': '1.0.0',
            'created_at': '2024-01-01T10:00:00+00:00',
            'updated_at': '2024-01-01T10:00:00+00:00'
        }
        
        # Convert DynamoDB item back to model
        restored = PipelineConfig.from_dynamodb_item(dynamodb_item)
        
        # Verify all boolean strings were converted to actual booleans recursively
        config = restored.spark_config
        
        # Level 1 conversions
        assert config['level1']['feature_enabled'] is True
        assert config['top_level_boolean'] is True
        
        # Level 2 conversions  
        assert config['level1']['level2']['nested_feature'] is False
        
        # Level 3 conversions
        assert config['level1']['level2']['level3']['deep_feature'] is True
        assert config['level1']['level2']['level3']['settings']['auto_optimize'] is False
        
        # Conversions in lists within nested dictionaries
        list_items = config['level1']['level2']['level3']['settings']['list_with_booleans']
        assert list_items[0]['enabled'] is True
        assert list_items[1]['enabled'] is False
        assert list_items[2]['active'] is True
        assert list_items[2]['name'] == 'test'  # Non-boolean should remain string
        
        # Top-level boolean conversion
        assert restored.is_active is True
        
        # Non-boolean strings should remain as strings
        assert config['regular_string'] == 'not_a_boolean'
        assert config['datetime_string'] == '2024-01-01T10:00:00Z'  # DateTimeMixin handles this
        
        # Verify datetime fields were handled by DateTimeMixin (not item_to_model)
        assert isinstance(restored.created_at, datetime)
        assert isinstance(restored.updated_at, datetime)

    def test_model_to_dynamodb_item_method(self):
        """Test the new to_dynamodb_item() method on models."""
        from decimal import Decimal
        from datetime import datetime, timezone
        
        # Create a pipeline with various data types
        pipeline = PipelineConfig(
            pipeline_id="test-to-dynamodb",
            pipeline_name="Test to_dynamodb_item Method",
            source_type="s3",
            destination_type="warehouse",
            created_at=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            memory_gb=Decimal("15.123456789"),
            is_active=True,
            spark_config={
                'nested_bool': False,
                'nested_dict': {
                    'deep_bool': True,
                    'deep_list': [
                        {'list_bool': False},
                        {'mixed': 'string', 'another_bool': True}
                    ]
                }
            }
        )
        
        # Test the new to_dynamodb_item() method
        dynamodb_item = pipeline.to_dynamodb_item()
        
        # Verify datetime conversion
        assert isinstance(dynamodb_item['created_at'], str)
        assert dynamodb_item['created_at'] == '2024-01-01T10:00:00+00:00'
        
        # Verify Decimal preservation
        assert isinstance(dynamodb_item['memory_gb'], Decimal)
        assert dynamodb_item['memory_gb'] == Decimal("15.123456789")
        
        # Verify boolean to string conversion
        assert isinstance(dynamodb_item['is_active'], str)
        assert dynamodb_item['is_active'] == 'true'
        
        # Verify nested boolean conversions
        assert dynamodb_item['spark_config']['nested_bool'] == 'false'
        assert dynamodb_item['spark_config']['nested_dict']['deep_bool'] == 'true'
        assert dynamodb_item['spark_config']['nested_dict']['deep_list'][0]['list_bool'] == 'false'
        assert dynamodb_item['spark_config']['nested_dict']['deep_list'][1]['another_bool'] == 'true'
        
        # Verify non-boolean strings remain unchanged
        assert dynamodb_item['spark_config']['nested_dict']['deep_list'][1]['mixed'] == 'string'
        
        # The canonical API is now the direct method call
        # No need to test deprecated utility function

    def test_canonical_api_usage(self):
        """Test the canonical API - single way to serialize/deserialize."""
        from decimal import Decimal
        from datetime import datetime, timezone
        
        # Create test pipeline
        original = PipelineConfig(
            pipeline_id="test-canonical-api",
            pipeline_name="Test Canonical API",
            source_type="s3",
            destination_type="warehouse",
            created_at=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            memory_gb=Decimal("15.5"),
            is_active=True,
            spark_config={
                'nested_bool': False,
                'deep_config': {
                    'another_bool': True,
                    'value': 42
                }
            }
        )
        
        # CANONICAL SERIALIZATION: pipeline.to_dynamodb_item()
        dynamo_item = original.to_dynamodb_item()
        
        # Verify serialization worked correctly
        assert dynamo_item['pipeline_id'] == "test-canonical-api"
        assert dynamo_item['created_at'] == "2024-01-01T10:00:00+00:00"
        assert dynamo_item['is_active'] == "true"  # Boolean converted to string
        assert dynamo_item['memory_gb'] == Decimal("15.5")  # Decimal preserved
        assert dynamo_item['spark_config']['nested_bool'] == "false"
        assert dynamo_item['spark_config']['deep_config']['another_bool'] == "true"
        
        # CANONICAL DESERIALIZATION: PipelineConfig.from_dynamodb_item()
        restored = PipelineConfig.from_dynamodb_item(dynamo_item)
        
        # Verify deserialization worked correctly
        assert restored.pipeline_id == original.pipeline_id
        assert restored.created_at == original.created_at
        assert restored.is_active == original.is_active
        assert restored.memory_gb == original.memory_gb
        assert restored.spark_config == original.spark_config
        
        # Verify type conversions
        assert isinstance(restored.created_at, datetime)
        assert isinstance(restored.is_active, bool)
        assert isinstance(restored.memory_gb, Decimal)
        assert isinstance(restored.spark_config['nested_bool'], bool)
        assert isinstance(restored.spark_config['deep_config']['another_bool'], bool)
        
        # Perfect roundtrip
        assert restored.pipeline_id == original.pipeline_id
        assert restored.pipeline_name == original.pipeline_name
        assert restored.spark_config == original.spark_config

    def test_model_from_dynamodb_item_method(self):
        """Test the new from_dynamodb_item() class method on models."""
        from decimal import Decimal
        from datetime import datetime, timezone
        
        # Simulate a DynamoDB item with DynamoDB-specific types
        dynamodb_item = {
            'pipeline_id': 'test-from-dynamodb',
            'pipeline_name': 'Test from_dynamodb_item Method',
            'source_type': 's3',
            'destination_type': 'warehouse',
            'created_at': '2024-01-01T10:00:00+00:00',  # ISO string from DynamoDB
            'memory_gb': Decimal("15.123456789"),  # Decimal from DynamoDB
            'is_active': 'true',  # String boolean from DynamoDB GSI
            'spark_config': {
                'nested_bool': 'false',  # String boolean
                'nested_dict': {
                    'deep_bool': 'true',  # String boolean
                    'deep_list': [
                        {'list_bool': 'false'},  # String boolean in list
                        {'mixed': 'string', 'another_bool': 'true'}  # Mixed types
                    ]
                }
            },
            'environment': 'dev',
            'version': '1.0.0'
        }
        
        # Test the new from_dynamodb_item() class method
        pipeline = PipelineConfig.from_dynamodb_item(dynamodb_item)
        
        # Verify datetime conversion (ISO string → datetime object)
        assert isinstance(pipeline.created_at, datetime)
        assert pipeline.created_at == datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        
        # Verify Decimal preservation
        assert isinstance(pipeline.memory_gb, Decimal)
        assert pipeline.memory_gb == Decimal("15.123456789")
        
        # Verify boolean string to boolean conversion
        assert isinstance(pipeline.is_active, bool)
        assert pipeline.is_active is True
        
        # Verify nested boolean string conversions
        assert pipeline.spark_config['nested_bool'] is False
        assert pipeline.spark_config['nested_dict']['deep_bool'] is True
        assert pipeline.spark_config['nested_dict']['deep_list'][0]['list_bool'] is False
        assert pipeline.spark_config['nested_dict']['deep_list'][1]['another_bool'] is True
        
        # Verify non-boolean strings remain unchanged
        assert pipeline.spark_config['nested_dict']['deep_list'][1]['mixed'] == 'string'
        
        # The canonical API is now the direct class method call
        # No need to test deprecated utility function

    def test_serialization_deserialization_roundtrip(self):
        """Test complete roundtrip: model → DynamoDB item → model."""
        from decimal import Decimal
        from datetime import datetime, timezone
        
        # Create original model
        original = PipelineConfig(
            pipeline_id="roundtrip-test",
            pipeline_name="Roundtrip Test",
            source_type="s3",
            destination_type="warehouse",
            created_at=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            memory_gb=Decimal("15.123456789"),
            is_active=True,
            spark_config={
                'nested_bool': False,
                'regular_string': 'should_remain_unchanged',
                'nested_dict': {
                    'deep_bool': True,
                    'number': 42
                }
            }
        )
        
        # Serialize to DynamoDB item
        dynamodb_item = original.to_dynamodb_item()
        
        # Deserialize back to model
        restored = PipelineConfig.from_dynamodb_item(dynamodb_item)
        
        # Verify roundtrip preservation
        assert restored.pipeline_id == original.pipeline_id
        assert restored.pipeline_name == original.pipeline_name
        assert restored.created_at == original.created_at
        assert restored.memory_gb == original.memory_gb
        assert restored.is_active == original.is_active
        assert restored.spark_config == original.spark_config
        
        # Verify specific type preservation
        assert isinstance(restored.created_at, datetime)
        assert isinstance(restored.memory_gb, Decimal)
        assert isinstance(restored.is_active, bool)
        assert isinstance(restored.spark_config['nested_bool'], bool)
        assert isinstance(restored.spark_config['nested_dict']['deep_bool'], bool)

    def test_list_of_dicts_serialization(self):
        """Test serialization of lists containing dictionaries."""
        schema_definition = {
            "fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "timestamp", "type": "timestamp", "nullable": True},
                {"name": "amount", "type": "decimal", "precision": 10, "scale": 2}
            ],
            "partitions": [
                {"column": "year", "type": "int"},
                {"column": "month", "type": "int"}
            ]
        }
        
        table = TableConfig(
            table_id="schema-test",
            pipeline_id="test-pipeline",
            table_name="test_table",
            table_type="source",
            data_format="parquet",
            location="s3://bucket/path/",
            schema_definition=schema_definition
        )
        
        item = table.to_dynamodb_item()
        restored = TableConfig.from_dynamodb_item(item)
        
        assert restored.schema_definition == schema_definition


class TestEdgeCaseValidation:
    """Test edge cases that might break type validation."""

    def test_extremely_long_strings(self):
        """Test handling of very long string values."""
        # Test string that approaches DynamoDB limits
        long_description = "x" * 10000  # 10KB string
        
        pipeline = PipelineConfig(
            pipeline_id="long-string-test",
            pipeline_name="Long String Test",
            source_type="s3",
            destination_type="warehouse",
            description=long_description
        )
        
        item = pipeline.to_dynamodb_item()
        restored = PipelineConfig.from_dynamodb_item(item)
        
        assert restored.description == long_description
        assert len(restored.description) == 10000

    def test_unicode_and_special_characters(self):
        """Test handling of Unicode and special characters."""
        unicode_test_cases = [
            "Hello 世界",  # Chinese characters
            "Café naïve résumé",  # Accented characters
            "🚀 🌟 ⭐",  # Emoji
            "Line1\\nLine2\\tTabbed",  # Escape sequences
            '{"json": "embedded"}',  # JSON-like string
            "<xml>embedded</xml>",  # XML-like string
        ]
        
        for test_string in unicode_test_cases:
            pipeline = PipelineConfig(
                pipeline_id="unicode-test",
                pipeline_name=test_string,
                source_type="s3",
                destination_type="warehouse"
            )
            
            item = pipeline.to_dynamodb_item()
            restored = PipelineConfig.from_dynamodb_item(item)
            
            assert restored.pipeline_name == test_string

    def test_boundary_numeric_values(self):
        """Test boundary numeric values."""
        boundary_cases = [
            0,
            1,
            -1,
            2**31 - 1,   # Max 32-bit signed int
            2**31,       # Min 32-bit signed int + 1
            2**63 - 1,   # Max 64-bit signed int
        ]
        
        for test_value in boundary_cases:
            try:
                pipeline = PipelineConfig(
                    pipeline_id="boundary-test",
                    pipeline_name="Boundary Test",
                    source_type="s3",
                    destination_type="warehouse",
                    cpu_cores=test_value
                )
                
                item = pipeline.to_dynamodb_item()
                restored = PipelineConfig.from_dynamodb_item(item)
                
                assert restored.cpu_cores == test_value
                
            except (ValidationError, PydanticValidationError, OverflowError):
                # Some boundary values may be rejected, which is acceptable
                print(f"Boundary value rejected: {test_value}")

    def test_mixed_type_dict_values(self):
        """Test dictionaries with mixed value types."""
        mixed_config = {
            "string_key": "string_value",
            "int_key": 42,
            "float_key": 3.14159,
            "bool_key": True,
            "none_key": None,
            "list_key": [1, "two", 3.0, True, None],
            "nested_dict": {
                "inner_string": "value",
                "inner_number": 123
            }
        }
        
        pipeline = PipelineConfig(
            pipeline_id="mixed-types-test",
            pipeline_name="Mixed Types Test",
            source_type="s3",
            destination_type="warehouse",
            spark_config=mixed_config
        )
        
        item = pipeline.to_dynamodb_item()
        restored = PipelineConfig.from_dynamodb_item(item)
        
        assert restored.spark_config == mixed_config

    def test_empty_collections(self):
        """Test handling of empty collections."""
        empty_cases = [
            ({}, "empty_dict"),
            ([], "empty_list"),
        ]
        
        for empty_value, description in empty_cases:
            if description == "empty_dict":
                pipeline = PipelineConfig(
                    pipeline_id="empty-dict-test",
                    pipeline_name="Empty Dict Test",
                    source_type="s3",
                    destination_type="warehouse",
                    spark_config=empty_value
                )
                
                item = pipeline.to_dynamodb_item()
                restored = PipelineConfig.from_dynamodb_item(item)
                
                assert restored.spark_config == empty_value
                
            elif description == "empty_list":
                table = TableConfig(
                    table_id="empty-list-test",
                    pipeline_id="test-pipeline",
                    table_name="test_table",
                    table_type="source",
                    data_format="parquet",
                    location="s3://bucket/path/",
                    partition_columns=empty_value
                )
                
                item = table.to_dynamodb_item()
                restored = TableConfig.from_dynamodb_item(item)
                
                assert restored.partition_columns == empty_value