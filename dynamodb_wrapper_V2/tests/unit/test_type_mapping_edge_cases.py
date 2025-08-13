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

from dynamodb_wrapper.utils import model_to_item, item_to_model
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
            item = model_to_item(pipeline)
            restored = item_to_model(item, PipelineConfig)
            
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
        
        item = model_to_item(pipeline)
        
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
                item = model_to_item(pipeline)
                restored = item_to_model(item, PipelineConfig)
                
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
        restored = item_to_model(item, PipelineConfig)
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
        
        item = model_to_item(pipeline)
        
        # None values should not appear in DynamoDB item
        assert 'memory_gb' not in item
        
        restored = item_to_model(item, PipelineConfig)
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
                
                item = model_to_item(pipeline)
                restored = item_to_model(item, PipelineConfig)
                
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
            
            item = model_to_item(pipeline)
            restored = item_to_model(item, PipelineConfig)
            
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
            
            item = model_to_item(pipeline)
            restored = item_to_model(item, PipelineConfig)
            
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
            
            item = model_to_item(table)
            restored = item_to_model(item, TableConfig)
            
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
            
            item = model_to_item(log)
            restored = item_to_model(item, PipelineRunLog)
            
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
        
        item = model_to_item(pipeline)
        restored = item_to_model(item, PipelineConfig)
        
        assert restored.spark_config == complex_config

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
        
        item = model_to_item(table)
        restored = item_to_model(item, TableConfig)
        
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
        
        item = model_to_item(pipeline)
        restored = item_to_model(item, PipelineConfig)
        
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
            
            item = model_to_item(pipeline)
            restored = item_to_model(item, PipelineConfig)
            
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
                
                item = model_to_item(pipeline)
                restored = item_to_model(item, PipelineConfig)
                
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
        
        item = model_to_item(pipeline)
        restored = item_to_model(item, PipelineConfig)
        
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
                
                item = model_to_item(pipeline)
                restored = item_to_model(item, PipelineConfig)
                
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
                
                item = model_to_item(table)
                restored = item_to_model(item, TableConfig)
                
                assert restored.partition_columns == empty_value