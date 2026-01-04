"""
Tests for Jira Adapter.
"""

import pytest
import responses
from responses import matchers

from waveql.adapters.jira import JiraAdapter
from waveql.query_planner import Predicate


@pytest.fixture
def adapter():
    """Create a Jira adapter for testing."""
    return JiraAdapter(host="https://test.atlassian.net")


class TestJiraAdapter:
    """Tests for JiraAdapter."""
    
    @responses.activate
    def test_fetch_issues(self, adapter):
        """Test fetching issues."""
        responses.add(
            responses.POST,
            "https://test.atlassian.net/rest/api/3/search",
            json={
                "issues": [
                    {
                        "id": "10001",
                        "key": "PROJ-1",
                        "fields": {
                            "summary": "Test Issue",
                            "status": {"name": "Open"},
                            "priority": {"name": "High"},
                            "assignee": {"displayName": "John Doe"},
                        }
                    },
                    {
                        "id": "10002",
                        "key": "PROJ-2",
                        "fields": {
                            "summary": "Another Issue",
                            "status": {"name": "In Progress"},
                            "priority": {"name": "Medium"},
                            "assignee": None,
                        }
                    }
                ],
                "total": 2,
                "maxResults": 100,
                "startAt": 0,
            },
            status=200,
        )
        
        result = adapter.fetch("issues")
        
        assert len(result) == 2
        records = result.to_pylist()
        assert records[0]["key"] == "PROJ-1"
        assert records[0]["summary"] == "Test Issue"
        # With struct support, nested objects are preserved as dicts
        assert records[0]["status"]["name"] == "Open"  # Access via struct
        assert records[1]["key"] == "PROJ-2"
        assert records[1]["assignee"] is None
    
    @responses.activate
    def test_fetch_with_jql_predicate(self, adapter):
        """Test predicate pushdown to JQL."""
        def request_callback(request):
            import json
            body = json.loads(request.body)
            # Verify JQL was constructed correctly
            assert 'project = "PROJ"' in body["jql"]
            assert 'status = "Open"' in body["jql"]
            
            return (200, {}, json.dumps({
                "issues": [{"id": "1", "key": "PROJ-1", "fields": {"summary": "Test"}}],
                "total": 1,
            }))
        
        responses.add_callback(
            responses.POST,
            "https://test.atlassian.net/rest/api/3/search",
            callback=request_callback,
            content_type="application/json",
        )
        
        predicates = [
            Predicate(column="project", operator="=", value="PROJ"),
            Predicate(column="status", operator="=", value="Open"),
        ]
        
        result = adapter.fetch("issues", predicates=predicates)
        assert len(result) == 1
    
    @responses.activate
    def test_fetch_with_like_predicate(self, adapter):
        """Test LIKE predicate conversion to JQL contains."""
        def request_callback(request):
            import json
            body = json.loads(request.body)
            # LIKE should be converted to ~ (contains)
            assert "summary ~" in body["jql"]
            
            return (200, {}, json.dumps({
                "issues": [],
                "total": 0,
            }))
        
        responses.add_callback(
            responses.POST,
            "https://test.atlassian.net/rest/api/3/search",
            callback=request_callback,
            content_type="application/json",
        )
        
        predicates = [
            Predicate(column="summary", operator="LIKE", value="%test%"),
        ]
        
        adapter.fetch("issues", predicates=predicates)
    
    @responses.activate
    def test_fetch_with_in_predicate(self, adapter):
        """Test IN predicate."""
        def request_callback(request):
            import json
            body = json.loads(request.body)
            assert "status IN" in body["jql"]
            
            return (200, {}, json.dumps({
                "issues": [],
                "total": 0,
            }))
        
        responses.add_callback(
            responses.POST,
            "https://test.atlassian.net/rest/api/3/search",
            callback=request_callback,
            content_type="application/json",
        )
        
        predicates = [
            Predicate(column="status", operator="IN", value=["Open", "In Progress"]),
        ]
        
        adapter.fetch("issues", predicates=predicates)
    
    @responses.activate
    def test_fetch_with_limit(self, adapter):
        """Test limit handling."""
        responses.add(
            responses.POST,
            "https://test.atlassian.net/rest/api/3/search",
            json={
                "issues": [
                    {"id": "1", "key": "PROJ-1", "fields": {"summary": "Issue 1"}},
                    {"id": "2", "key": "PROJ-2", "fields": {"summary": "Issue 2"}},
                    {"id": "3", "key": "PROJ-3", "fields": {"summary": "Issue 3"}},
                ],
                "total": 3,
            },
            status=200,
        )
        
        result = adapter.fetch("issues", limit=2)
        assert len(result) == 2
    
    @responses.activate
    def test_fetch_with_order_by(self, adapter):
        """Test ORDER BY in JQL."""
        def request_callback(request):
            import json
            body = json.loads(request.body)
            assert "ORDER BY created DESC" in body["jql"]
            
            return (200, {}, json.dumps({
                "issues": [],
                "total": 0,
            }))
        
        responses.add_callback(
            responses.POST,
            "https://test.atlassian.net/rest/api/3/search",
            callback=request_callback,
            content_type="application/json",
        )
        
        adapter.fetch("issues", order_by=[("created", "DESC")])
    
    @responses.activate
    def test_fetch_projects(self, adapter):
        """Test fetching projects (non-JQL endpoint)."""
        responses.add(
            responses.GET,
            "https://test.atlassian.net/rest/api/3/project/search",
            json={
                "values": [
                    {"id": "1", "key": "PROJ", "name": "Test Project"},
                    {"id": "2", "key": "DEV", "name": "Development"},
                ]
            },
            status=200,
        )
        
        result = adapter.fetch("projects")
        
        assert len(result) == 2
        records = result.to_pylist()
        assert records[0]["key"] == "PROJ"
    
    @responses.activate
    def test_insert_issue(self, adapter):
        """Test creating a new issue."""
        responses.add(
            responses.POST,
            "https://test.atlassian.net/rest/api/3/issue",
            json={"id": "10001", "key": "PROJ-1"},
            status=201,
        )
        
        values = {
            "project": {"key": "PROJ"},
            "issuetype": {"name": "Task"},
            "summary": "New test issue",
        }
        
        result = adapter.insert("issues", values)
        assert result == 1
    
    @responses.activate
    def test_update_issue(self, adapter):
        """Test updating an issue."""
        responses.add(
            responses.PUT,
            "https://test.atlassian.net/rest/api/3/issue/PROJ-1",
            status=204,
        )
        
        values = {"summary": "Updated summary"}
        predicates = [Predicate(column="key", operator="=", value="PROJ-1")]
        
        result = adapter.update("issues", values, predicates)
        assert result == 1
    
    def test_update_requires_key(self, adapter):
        """Test that UPDATE requires key in WHERE clause."""
        with pytest.raises(Exception) as excinfo:
            adapter.update("issues", {"summary": "Test"}, predicates=[])
        assert "key" in str(excinfo.value).lower() or "id" in str(excinfo.value).lower()
    
    @responses.activate
    def test_delete_issue(self, adapter):
        """Test deleting an issue."""
        responses.add(
            responses.DELETE,
            "https://test.atlassian.net/rest/api/3/issue/PROJ-1",
            status=204,
        )
        
        predicates = [Predicate(column="key", operator="=", value="PROJ-1")]
        
        result = adapter.delete("issues", predicates)
        assert result == 1
    
    def test_delete_requires_key(self, adapter):
        """Test that DELETE requires key in WHERE clause."""
        with pytest.raises(Exception) as excinfo:
            adapter.delete("issues", predicates=[])
        assert "key" in str(excinfo.value).lower() or "id" in str(excinfo.value).lower()
    
    @responses.activate
    def test_pagination(self, adapter):
        """Test pagination handling."""
        # First page
        responses.add(
            responses.POST,
            "https://test.atlassian.net/rest/api/3/search",
            json={
                "issues": [{"id": str(i), "key": f"PROJ-{i}", "fields": {}} for i in range(1, 101)],
                "total": 150,
                "startAt": 0,
            },
            status=200,
        )
        
        # Second page
        responses.add(
            responses.POST,
            "https://test.atlassian.net/rest/api/3/search",
            json={
                "issues": [{"id": str(i), "key": f"PROJ-{i}", "fields": {}} for i in range(101, 151)],
                "total": 150,
                "startAt": 100,
            },
            status=200,
        )
        
        result = adapter.fetch("issues")
        assert len(result) == 150
    
    def test_list_tables(self, adapter):
        """Test listing available tables."""
        tables = adapter.list_tables()
        
        assert "issue" in tables
        assert "issues" in tables
        assert "project" in tables
        assert "user" in tables
    
    def test_host_normalization(self):
        """Test that host URL is normalized."""
        adapter1 = JiraAdapter(host="test.atlassian.net")
        assert adapter1._host == "https://test.atlassian.net"
        
        adapter2 = JiraAdapter(host="https://test.atlassian.net/")
        assert adapter2._host == "https://test.atlassian.net"
    
    def test_table_name_extraction(self, adapter):
        """Test table name extraction from schema.table format."""
        assert adapter._extract_table_name("jira.issues") == "issues"
        assert adapter._extract_table_name("issues") == "issues"
        assert adapter._extract_table_name("ISSUES") == "issues"


class TestJQLConversion:
    """Tests for JQL query construction."""
    
    @pytest.fixture
    def adapter(self):
        return JiraAdapter(host="https://test.atlassian.net")
    
    def test_equals_predicate(self, adapter):
        """Test equals operator."""
        pred = Predicate(column="project", operator="=", value="PROJ")
        jql = adapter._predicate_to_jql(pred)
        assert jql == 'project = "PROJ"'
    
    def test_not_equals_predicate(self, adapter):
        """Test not equals operator."""
        pred = Predicate(column="status", operator="!=", value="Done")
        jql = adapter._predicate_to_jql(pred)
        assert jql == 'status != "Done"'
    
    def test_comparison_predicates(self, adapter):
        """Test comparison operators."""
        pred_gt = Predicate(column="priority", operator=">", value=3)
        assert adapter._predicate_to_jql(pred_gt) == "priority > 3"
        
        pred_lt = Predicate(column="priority", operator="<", value=3)
        assert adapter._predicate_to_jql(pred_lt) == "priority < 3"
    
    def test_is_null_predicate(self, adapter):
        """Test IS NULL."""
        pred = Predicate(column="assignee", operator="IS NULL", value=None)
        jql = adapter._predicate_to_jql(pred)
        assert jql == "assignee IS EMPTY"
    
    def test_is_not_null_predicate(self, adapter):
        """Test IS NOT NULL."""
        pred = Predicate(column="assignee", operator="IS NOT NULL", value=None)
        jql = adapter._predicate_to_jql(pred)
        assert jql == "assignee IS NOT EMPTY"
    
    def test_like_predicate(self, adapter):
        """Test LIKE conversion to contains."""
        pred = Predicate(column="summary", operator="LIKE", value="%test%")
        jql = adapter._predicate_to_jql(pred)
        assert 'summary ~ "test"' in jql
    
    def test_in_predicate_list(self, adapter):
        """Test IN with list."""
        pred = Predicate(column="status", operator="IN", value=["Open", "In Progress"])
        jql = adapter._predicate_to_jql(pred)
        assert "status IN" in jql
        assert "Open" in jql
        assert "In Progress" in jql


class TestIssueNormalization:
    """Tests for issue normalization logic (preserves nested structures)."""
    
    @pytest.fixture
    def adapter(self):
        return JiraAdapter(host="https://test.atlassian.net")
    
    def test_normalize_basic_fields(self, adapter):
        """Test normalizing basic issue fields."""
        issue = {
            "id": "10001",
            "key": "PROJ-1",
            "fields": {
                "summary": "Test summary",
                "description": "Test description",
            }
        }
        
        normalized = adapter._normalize_issue(issue)
        
        assert normalized["id"] == "10001"
        assert normalized["key"] == "PROJ-1"
        assert normalized["summary"] == "Test summary"
    
    def test_normalize_preserves_nested_objects(self, adapter):
        """Test that nested objects are preserved as dicts (for struct columns)."""
        issue = {
            "id": "10001",
            "key": "PROJ-1",
            "fields": {
                "status": {"name": "Open"},
                "priority": {"name": "High"},
                "assignee": {"displayName": "John Doe"},
                "project": {"key": "PROJ"},
            }
        }
        
        normalized = adapter._normalize_issue(issue)
        
        # Nested objects should be preserved, not flattened
        assert normalized["status"] == {"name": "Open"}
        assert normalized["priority"] == {"name": "High"}
        assert normalized["assignee"] == {"displayName": "John Doe"}
        assert normalized["project"] == {"key": "PROJ"}
    
    def test_normalize_null_values(self, adapter):
        """Test normalizing null values."""
        issue = {
            "id": "10001",
            "key": "PROJ-1",
            "fields": {
                "assignee": None,
                "duedate": None,
            }
        }
        
        normalized = adapter._normalize_issue(issue)
        
        assert normalized["assignee"] is None
        assert normalized["duedate"] is None
    
    def test_normalize_preserves_arrays(self, adapter):
        """Test that arrays are preserved as lists (for list columns)."""
        issue = {
            "id": "10001",
            "key": "PROJ-1",
            "fields": {
                "labels": ["bug", "critical"],
                "components": [{"name": "Backend"}, {"name": "API"}],
            }
        }
        
        normalized = adapter._normalize_issue(issue)
        
        # Arrays should be preserved as-is
        assert normalized["labels"] == ["bug", "critical"]
        assert normalized["components"] == [{"name": "Backend"}, {"name": "API"}]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
