"""
Integration tests for model-driven key management system.

These tests verify that the model-aware key building functions work correctly
with the actual domain models and provide a better developer experience.
"""

import pytest
from datetime import datetime, timezone

from dynamodb_wrapper.models.domain_models import PipelineConfig, TableConfig, PipelineRunLog
from dynamodb_wrapper.utils import (
    build_model_key,
    build_model_key_condition, 
    build_gsi_key_condition,
    get_model_primary_key_fields,
    get_model_gsi_names
)


class TestModelMetadata:
    """Test model metadata extraction."""
    
    def test_pipeline_config_metadata(self):
        """Test PipelineConfig metadata extraction."""
        assert get_model_primary_key_fields(PipelineConfig) == ['pipeline_id']
        assert 'ActivePipelinesIndex' in get_model_gsi_names(PipelineConfig)
        assert 'EnvironmentIndex' in get_model_gsi_names(PipelineConfig)
    
    def test_table_config_metadata(self):
        """Test TableConfig metadata extraction."""
        assert get_model_primary_key_fields(TableConfig) == ['table_id']
        assert 'PipelineTablesIndex' in get_model_gsi_names(TableConfig)
        assert 'TableTypeIndex' in get_model_gsi_names(TableConfig)
    
    def test_pipeline_run_log_metadata(self):
        """Test PipelineRunLog metadata extraction."""
        assert get_model_primary_key_fields(PipelineRunLog) == ['run_id', 'pipeline_id']
        assert 'PipelineRunsIndex' in get_model_gsi_names(PipelineRunLog)
        assert 'StatusRunsIndex' in get_model_gsi_names(PipelineRunLog)


class TestModelDrivenKeyBuilding:
    """Test model-driven key building integration."""
    
    def test_pipeline_config_key_building(self):
        """Test building keys for PipelineConfig model."""
        # Simple primary key
        key = build_model_key(PipelineConfig, pipeline_id="test-pipeline-123")
        assert key == {"pipeline_id": "test-pipeline-123"}
        
        # Primary key condition
        condition = build_model_key_condition(PipelineConfig, pipeline_id="test-pipeline-123")
        assert condition is not None
        
        # GSI condition
        gsi_condition = build_gsi_key_condition(
            PipelineConfig, 
            "ActivePipelinesIndex", 
            is_active=True
        )
        assert gsi_condition is not None
    
    def test_table_config_key_building(self):
        """Test building keys for TableConfig model."""
        # Simple primary key
        key = build_model_key(TableConfig, table_id="test-table-456")
        assert key == {"table_id": "test-table-456"}
        
        # GSI condition with sort key
        gsi_condition = build_gsi_key_condition(
            TableConfig,
            "PipelineTablesIndex", 
            pipeline_id="test-pipeline-123",
            table_type="source"
        )
        assert gsi_condition is not None
    
    def test_pipeline_run_log_key_building(self):
        """Test building keys for PipelineRunLog composite model."""
        # Composite primary key
        key = build_model_key(
            PipelineRunLog, 
            run_id="test-run-789",
            pipeline_id="test-pipeline-123"
        )
        assert key == {
            "run_id": "test-run-789", 
            "pipeline_id": "test-pipeline-123"
        }
        
        # Composite key condition  
        condition = build_model_key_condition(
            PipelineRunLog,
            run_id="test-run-789",
            pipeline_id="test-pipeline-123"
        )
        assert condition is not None
        
        # GSI condition
        gsi_condition = build_gsi_key_condition(
            PipelineRunLog,
            "StatusRunsIndex",
            status="running"
        )
        assert gsi_condition is not None


class TestErrorHandling:
    """Test error handling in model-driven key building."""
    
    def test_missing_required_key_fields(self):
        """Test error when required key fields are missing."""
        # Missing partition key
        with pytest.raises(ValueError, match="Missing partition key 'pipeline_id'"):
            build_model_key(PipelineConfig)
        
        # Missing sort key for composite model
        with pytest.raises(ValueError, match="Missing sort key 'pipeline_id'"):
            build_model_key(PipelineRunLog, run_id="test-run")
    
    def test_invalid_gsi_name(self):
        """Test error when GSI name doesn't exist."""
        with pytest.raises(ValueError, match="GSI 'InvalidIndex' not found"):
            build_gsi_key_condition(PipelineConfig, "InvalidIndex", is_active=True)
    
    def test_missing_gsi_partition_key(self):
        """Test error when GSI partition key is missing."""
        with pytest.raises(ValueError, match="Missing GSI partition key 'is_active'"):
            build_gsi_key_condition(PipelineConfig, "ActivePipelinesIndex")
    
    def test_model_without_meta_class(self):
        """Test error when model doesn't have Meta class."""
        from pydantic import BaseModel
        
        class ModelWithoutMeta(BaseModel):
            id: str
            name: str
        
        with pytest.raises(ValueError, match="Model ModelWithoutMeta must have a Meta class"):
            build_model_key(ModelWithoutMeta, id="test")
        
        with pytest.raises(ValueError, match="Model ModelWithoutMeta must have a Meta class"):
            get_model_primary_key_fields(ModelWithoutMeta)
        
        with pytest.raises(ValueError, match="Model ModelWithoutMeta must have a Meta class"):
            get_model_gsi_names(ModelWithoutMeta)
    
    def test_meta_class_without_partition_key(self):
        """Test error when Meta class doesn't define partition_key."""
        from pydantic import BaseModel
        
        class ModelWithIncompleteMeta(BaseModel):
            id: str
            name: str
            
            class Meta:
                # Missing partition_key
                sort_key = None
                gsis = []
        
        with pytest.raises(ValueError, match="Model ModelWithIncompleteMeta.Meta must define partition_key"):
            build_model_key(ModelWithIncompleteMeta, id="test")


class TestDeveloperExperience:
    """Test developer experience improvements."""
    
    def test_type_safety_through_model_reference(self):
        """Test that using model classes provides better type safety."""
        # The fact that we pass the model class ensures compile-time checking
        # of available fields in modern IDEs
        
        # This should work (valid field)
        key = build_model_key(PipelineConfig, pipeline_id="test")
        assert key["pipeline_id"] == "test"
        
        # This would be caught by IDE/type checker (if we had full typing)
        # build_model_key(PipelineConfig, invalid_field="test")  # Would be flagged
    
    def test_single_source_of_truth_for_keys(self):
        """Test that key definitions come from model metadata."""
        # Key structure is defined in the model, not repeated in code
        pipeline_keys = get_model_primary_key_fields(PipelineConfig)
        run_log_keys = get_model_primary_key_fields(PipelineRunLog)
        
        # Different models have different key structures
        assert len(pipeline_keys) == 1  # Simple key
        assert len(run_log_keys) == 2   # Composite key
        
        # GSI definitions are also centralized
        pipeline_gsis = get_model_gsi_names(PipelineConfig) 
        assert "ActivePipelinesIndex" in pipeline_gsis
        assert "EnvironmentIndex" in pipeline_gsis
        
        # If we add a new GSI to the model, it's immediately available
        # without updating utility functions