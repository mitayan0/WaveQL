"""Tests for saved queries."""

import pytest
from waveql.semantic.saved_queries import SavedQuery, SavedQueryRegistry


class TestSavedQuery:
    def test_create(self):
        q = SavedQuery(name="test", sql="SELECT 1")
        assert q.name == "test"

    def test_to_dict(self):
        q = SavedQuery(name="test", sql="SELECT 1")
        d = q.to_dict()
        assert d["name"] == "test"


class TestSavedQueryRegistry:
    def test_register_get(self):
        reg = SavedQueryRegistry()
        q = SavedQuery(name="test", sql="SELECT 1")
        reg.register(q)
        assert reg.get("test") is not None

    def test_list(self):
        reg = SavedQueryRegistry()
        reg.register(SavedQuery(name="q1", sql="SELECT 1"))
        reg.register(SavedQuery(name="q2", sql="SELECT 2"))
        queries = reg.list_queries()
        assert len(queries) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
