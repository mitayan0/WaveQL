"""
Tests for Salesforce Adapter
"""

import pytest
import responses
import pyarrow as pa
from waveql.adapters.salesforce import SalesforceAdapter
from waveql.query_planner import Predicate
from waveql.exceptions import AdapterError, QueryError

MOCK_ACCOUNTS = [
    {
        "Id": "001xx000003DHP0AAO",
        "Name": "Edge Communications",
        "Type": "Customer - Direct",
        "AnnualRevenue": 139000000,
    },
    {
        "Id": "001xx000003DHP1AAO",
        "Name": "Burlington Textiles Corp of America",
        "Type": "Customer - Direct",
        "AnnualRevenue": 350000000,
    }
]

MOCK_DESCRIBE_ACCOUNT = {
    "name": "Account",
    "fields": [
        {"name": "Id", "type": "id", "nillable": False},
        {"name": "Name", "type": "string", "nillable": True},
        {"name": "Type", "type": "picklist", "nillable": True},
        {"name": "AnnualRevenue", "type": "currency", "nillable": True},
    ]
}

class TestSalesforceAdapter:
    
    @responses.activate
    def test_fetch_query_construction(self):
        """Test that simple fetch constructs correct SOQL."""
        # Mock Describe (called first for schema if cols=*)
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/sobjects/Account/describe",
            json=MOCK_DESCRIBE_ACCOUNT,
            status=200
        )
        
        # Mock Query
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/query",
            json={"done": True, "totalSize": 2, "records": MOCK_ACCOUNTS},
            status=200
        )
        
        adapter = SalesforceAdapter(host="test.salesforce.com")
        adapter.fetch("Account")
        
        # Verify Query URL
        call = responses.calls[1] # 0 is describe, 1 is query
        assert "q=SELECT+Id%2C+Name%2C+Type%2C+AnnualRevenue+FROM+Account" in call.request.url

    @responses.activate
    def test_fetch_with_columns(self):
        """Test fetching with specific columns (skips describe)."""
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/query",
            json={"done": True, "totalSize": 2, "records": MOCK_ACCOUNTS},
            status=200
        )
        
        # Mock describe for _to_arrow schema building
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/sobjects/Account/describe",
            json=MOCK_DESCRIBE_ACCOUNT,
            status=200
        )

        adapter = SalesforceAdapter(host="test.salesforce.com")
        adapter.fetch("Account", columns=["Name", "AnnualRevenue"])
        
        assert "SELECT+Name%2C+AnnualRevenue+FROM+Account" in responses.calls[0].request.url

    @responses.activate
    def test_predicate_translation(self):
        """Test translation of WaveQL predicates to SOQL."""
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/query",
            json={"done": True, "totalSize": 1, "records": [MOCK_ACCOUNTS[0]]},
            status=200
        )
        
        # We need to mock describe because fetch will try to get schema for arrow conversion if not explicitly provided
        # Or we can just let it hit the describe endpoint if we don't pass schema cache.
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/sobjects/Account/describe",
            json=MOCK_DESCRIBE_ACCOUNT,
            status=200
        )

        adapter = SalesforceAdapter(host="test.salesforce.com")
        predicates = [
            Predicate("Name", "LIKE", "%Edge%"),
            Predicate("AnnualRevenue", ">", 1000000)
        ]
        
        adapter.fetch("Account", columns=["Id"], predicates=predicates)
        
        from urllib.parse import unquote_plus
        url = unquote_plus(responses.calls[0].request.url)
        assert "Name LIKE '%Edge%'" in url
        assert "AnnualRevenue > 1000000" in url

    @responses.activate
    def test_pagination(self):
        """Test following nextRecordsUrl."""
        # Page 1
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/query",
            json={
                "done": False, 
                "totalSize": 2000, 
                "records": [MOCK_ACCOUNTS[0]],
                "nextRecordsUrl": "/services/data/v57.0/query/01g..."
            },
            status=200
        )
        
        # Page 2
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/query/01g...",
            json={
                "done": True, 
                "totalSize": 2000, 
                "records": [MOCK_ACCOUNTS[1]]
            },
            status=200
        )
        
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/sobjects/Account/describe",
            json=MOCK_DESCRIBE_ACCOUNT,
            status=200
        )

        adapter = SalesforceAdapter(host="test.salesforce.com")
        result = adapter.fetch("Account", columns=["Id"])
        
        assert len(result) == 2
        assert len(responses.calls) == 3 # Page 1, Page 2, Describe

    @responses.activate
    def test_crud_insert(self):
        """Test INSERT."""
        responses.add(
            responses.POST,
            "https://test.salesforce.com/services/data/v57.0/sobjects/Account",
            json={"id": "001xx000003DHP0AAO", "success": True, "errors": []},
            status=201
        )
        
        adapter = SalesforceAdapter(host="test.salesforce.com")
        adapter.insert("Account", {"Name": "New Account"})
        
        assert responses.calls[0].request.method == "POST"

    @responses.activate
    def test_crud_update(self):
        """Test UPDATE."""
        responses.add(
            responses.PATCH,
            "https://test.salesforce.com/services/data/v57.0/sobjects/Account/001xx000003DHP0AAO",
            status=204
        )
        
        adapter = SalesforceAdapter(host="test.salesforce.com")
        pred = [Predicate("Id", "=", "001xx000003DHP0AAO")]
        adapter.update("Account", {"Name": "Updated Name"}, predicates=pred)
        
        assert responses.calls[0].request.method == "PATCH"
        assert "001xx000003DHP0AAO" in responses.calls[0].request.url

    @responses.activate
    def test_crud_delete(self):
        """Test DELETE."""
        responses.add(
            responses.DELETE,
            "https://test.salesforce.com/services/data/v57.0/sobjects/Account/001xx000003DHP0AAO",
            status=204
        )
        
        adapter = SalesforceAdapter(host="test.salesforce.com")
        pred = [Predicate("Id", "=", "001xx000003DHP0AAO")]
        adapter.delete("Account", predicates=pred)
        
        assert responses.calls[0].request.method == "DELETE"

    @responses.activate
    def test_bulk_insert_success(self):
        """Test Bulk API V2 Insert success flow."""
        # 1. Create Job
        responses.add(
            responses.POST,
            "https://test.salesforce.com/services/data/v57.0/jobs/ingest/",
            json={"id": "job123", "state": "Open"},
            status=200
        )
        
        # 2. Upload Data
        responses.add(
            responses.PUT,
            "https://test.salesforce.com/services/data/v57.0/jobs/ingest/job123/batches",
            status=201
        )
        
        # 3. Close Job (UploadComplete)
        responses.add(
            responses.PATCH,
            "https://test.salesforce.com/services/data/v57.0/jobs/ingest/job123/",
            json={"id": "job123", "state": "UploadComplete"},
            status=200
        )
        
        # 4. Check Status (Polling - return processing then complete)
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/jobs/ingest/job123/",
            json={"id": "job123", "state": "InProgress"},
            status=200
        )
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/jobs/ingest/job123/",
            json={"id": "job123", "state": "JobComplete", "numberRecordsProcessed": 10},
            status=200
        )
        
        adapter = SalesforceAdapter(host="test.salesforce.com")
        records = [{"Name": f"Account {i}"} for i in range(10)]
        
        # Mock time.sleep to run fast
        import time
        original_sleep = time.sleep
        time.sleep = lambda x: None
        
        try:
            status = adapter.insert_bulk("Account", records)
            
            assert status["state"] == "JobComplete"
            assert status["numberRecordsProcessed"] == 10
            
            # Verify Flow
            assert len(responses.calls) == 5
            # PUT request should have CSV content type
            assert responses.calls[1].request.headers["Content-Type"] == "text/csv"
            # And body should look like CSV
            assert "Name" in responses.calls[1].request.body
            
        finally:
            time.sleep = original_sleep

    @responses.activate
    def test_aggregation_pushdown(self):
        """Test aggregation query generation."""
        from waveql.query_planner import Aggregate
        from urllib.parse import unquote_plus
        
        # 1. Query
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/query",
            json={
                "done": True,
                "totalSize": 2,
                "records": [
                    {"Industry": "Tech", "cnt": 10},
                    {"Industry": "Retail", "cnt": 5},
                ]
            },
            status=200
        )
        
        # 2. Describe (invoked by _build_aggregate_schema)
        responses.add(
            responses.GET,
            "https://test.salesforce.com/services/data/v57.0/sobjects/Account/describe",
            json={
                "name": "Account",
                "fields": [
                   {"name": "Industry", "type": "picklist", "nillable": True},
                   {"name": "Id", "type": "id", "nillable": False}
                ]
            },
            status=200
        )
        
        adapter = SalesforceAdapter(host="test.salesforce.com")
        
        data = adapter.fetch(
            "Account",
            group_by=["Industry"],
            aggregates=[Aggregate("COUNT", "Id", "cnt")]
        )
        
        # Verify URL construction
        # Note: calls[0] is query because fetch logic calls execute before schema for aggregations
        url = unquote_plus(responses.calls[0].request.url)
        # Expected: SELECT Industry, COUNT(Id) cnt FROM Account GROUP BY Industry
        # We check parts because order of fields might vary if not set
        assert "SELECT Industry, COUNT(Id) cnt FROM Account" in url
        assert "GROUP BY Industry" in url
        
        # Verify result Arrow table
        assert len(data) == 2
        assert data.column_names == ["Industry", "cnt"]
        assert data["cnt"][0].as_py() == 10
        assert data["Industry"][0].as_py() == "Tech"

