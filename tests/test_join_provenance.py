"""
Tests for How-Provenance: Join Transformation Tracking

This tests the join provenance tracking feature that records how data
from multiple tables was combined during query execution.
"""

import pytest
from datetime import datetime

from waveql.provenance import (
    enable_provenance,
    disable_provenance,
    get_provenance_tracker,
    JoinTransformation,
    QueryProvenance,
)


class TestJoinTransformation:
    """Test the JoinTransformation dataclass."""
    
    def test_join_transformation_creation(self):
        """Test creating a JoinTransformation."""
        jt = JoinTransformation(
            left_table="servicenow.incident",
            right_table="salesforce.contact",
            left_column="caller_id",
            right_column="Id",
            join_type="LEFT",
        )
        
        assert jt.left_table == "servicenow.incident"
        assert jt.right_table == "salesforce.contact"
        assert jt.left_column == "caller_id"
        assert jt.right_column == "Id"
        assert jt.join_type == "LEFT"
        assert jt.timestamp is not None
    
    def test_join_transformation_defaults(self):
        """Test JoinTransformation defaults."""
        jt = JoinTransformation(
            left_table="t1",
            right_table="t2",
            left_column="id",
            right_column="id",
        )
        
        assert jt.join_type == "INNER"  # Default
        assert isinstance(jt.timestamp, datetime)
    
    def test_join_transformation_repr(self):
        """Test JoinTransformation repr."""
        jt = JoinTransformation(
            left_table="t1",
            right_table="t2",
            left_column="c1",
            right_column="c2",
            join_type="LEFT",
        )
        
        repr_str = repr(jt)
        assert "JoinTransformation" in repr_str
        assert "t1.c1" in repr_str
        assert "t2.c2" in repr_str
        assert "LEFT JOIN" in repr_str
    
    def test_join_transformation_to_dict(self):
        """Test JoinTransformation serialization."""
        jt = JoinTransformation(
            left_table="servicenow.incident",
            right_table="salesforce.contact",
            left_column="caller_id",
            right_column="Id",
            join_type="INNER",
        )
        
        d = jt.to_dict()
        assert d["left_table"] == "servicenow.incident"
        assert d["right_table"] == "salesforce.contact"
        assert d["left_column"] == "caller_id"
        assert d["right_column"] == "Id"
        assert d["join_type"] == "INNER"
        assert "timestamp" in d


class TestQueryProvenanceJoinTracking:
    """Test QueryProvenance with join transformations."""
    
    def test_query_provenance_has_join_transformations(self):
        """Test QueryProvenance has join_transformations field."""
        prov = QueryProvenance(original_sql="SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
        
        assert hasattr(prov, "join_transformations")
        assert isinstance(prov.join_transformations, list)
        assert len(prov.join_transformations) == 0
    
    def test_query_provenance_repr_includes_joins(self):
        """Test QueryProvenance repr includes join count."""
        prov = QueryProvenance(original_sql="SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
        prov.join_transformations.append(
            JoinTransformation("t1", "t2", "id", "id", "INNER")
        )
        
        repr_str = repr(prov)
        assert "joins=1" in repr_str
    
    def test_query_provenance_to_dict_includes_joins(self):
        """Test QueryProvenance.to_dict() includes join_transformations."""
        prov = QueryProvenance(original_sql="SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
        prov.join_transformations.append(
            JoinTransformation("t1", "t2", "id", "id", "INNER")
        )
        
        d = prov.to_dict()
        assert "join_transformations" in d
        assert len(d["join_transformations"]) == 1
        assert d["join_transformations"][0]["left_table"] == "t1"


class TestProvenanceTrackerJoinRecording:
    """Test ProvenanceTracker.record_join_transformation()."""
    
    def setup_method(self):
        """Reset tracker before each test."""
        tracker = get_provenance_tracker()
        disable_provenance()
        tracker.clear_history()
    
    def test_record_join_transformation_basic(self):
        """Test recording a join transformation."""
        tracker = get_provenance_tracker()
        enable_provenance(mode="full")
        
        with tracker.trace_query("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id") as prov:
            tracker.record_join_transformation(
                left_table="t1",
                right_table="t2",
                left_column="id",
                right_column="id",
                join_type="INNER",
            )
        
        assert len(prov.join_transformations) == 1
        jt = prov.join_transformations[0]
        assert jt.left_table == "t1"
        assert jt.right_table == "t2"
        assert jt.join_type == "INNER"
    
    def test_record_join_transformation_multiple_joins(self):
        """Test recording multiple join transformations."""
        tracker = get_provenance_tracker()
        enable_provenance(mode="full")
        
        with tracker.trace_query("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id") as prov:
            tracker.record_join_transformation("t1", "t2", "id", "id", "INNER")
            tracker.record_join_transformation("t2", "t3", "id", "id", "LEFT")
        
        assert len(prov.join_transformations) == 2
        assert prov.join_transformations[0].join_type == "INNER"
        assert prov.join_transformations[1].join_type == "LEFT"
    
    def test_record_join_transformation_disabled(self):
        """Test that join recording is skipped when disabled."""
        tracker = get_provenance_tracker()
        disable_provenance()
        
        with tracker.trace_query("SELECT * FROM t1") as prov:
            tracker.record_join_transformation("t1", "t2", "id", "id")
        
        # When disabled, prov is None
        assert prov is None
    
    def test_record_join_transformation_no_active_query(self):
        """Test that join recording is skipped with no active query."""
        tracker = get_provenance_tracker()
        enable_provenance(mode="full")
        
        # No active trace_query context
        result = tracker.record_join_transformation("t1", "t2", "id", "id")
        assert result is None  # Should return None silently
    
    def test_record_join_transformation_cross_adapter(self):
        """Test recording cross-adapter joins."""
        tracker = get_provenance_tracker()
        enable_provenance(mode="full")
        
        with tracker.trace_query("SELECT * FROM servicenow.incident i JOIN salesforce.contact c ON i.caller_id = c.Id") as prov:
            tracker.record_join_transformation(
                left_table="servicenow.incident",
                right_table="salesforce.contact",
                left_column="caller_id",
                right_column="Id",
                join_type="LEFT",
            )
        
        assert len(prov.join_transformations) == 1
        jt = prov.join_transformations[0]
        assert jt.left_table == "servicenow.incident"
        assert jt.right_table == "salesforce.contact"


class TestJoinProvenanceSerialization:
    """Test serialization of join provenance."""
    
    def test_full_provenance_dict_structure(self):
        """Test complete provenance dict includes all fields."""
        prov = QueryProvenance(
            original_sql="SELECT * FROM t1 JOIN t2 ON t1.id = t2.id",
            provenance_mode="full",
        )
        
        # Add a join transformation
        prov.join_transformations.append(
            JoinTransformation("t1", "t2", "id", "id", "INNER")
        )
        
        d = prov.to_dict()
        
        # Verify structure
        assert "query_id" in d
        assert "original_sql" in d
        assert "api_calls" in d
        assert "join_transformations" in d
        assert "adapters_used" in d
        assert "tables_accessed" in d
        
        # Verify join transformation structure
        jt = d["join_transformations"][0]
        assert "left_table" in jt
        assert "right_table" in jt
        assert "left_column" in jt
        assert "right_column" in jt
        assert "join_type" in jt
        assert "timestamp" in jt


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
