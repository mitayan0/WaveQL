"""
Unit tests for Client-Side Aggregation functionality.

Tests cover:
1. BaseAdapter aggregation methods (_streaming_aggregate, _aggregate_with_groupby, _compute_approximate_aggregates)
2. Smart COUNT optimization for HubSpot, Shopify, Zendesk, Stripe
3. Performance warnings for large datasets
4. Edge cases (empty tables, missing columns, null values)
"""

import pytest
import respx
import httpx
import pyarrow as pa
import logging
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

from waveql.adapters.base import BaseAdapter
from waveql.adapters.hubspot import HubSpotAdapter
from waveql.adapters.shopify import ShopifyAdapter
from waveql.adapters.zendesk import ZendeskAdapter
from waveql.adapters.stripe import StripeAdapter
from waveql.query_planner import Predicate


# Mock Aggregate class for testing
@dataclass
class MockAggregate:
    func: str
    column: str
    alias: str = None


# --- BaseAdapter Aggregation Tests ---

class TestBaseAdapterAggregation:
    """Tests for BaseAdapter aggregation methods."""
    
    @pytest.fixture
    def adapter(self):
        """Create a minimal BaseAdapter for testing."""
        # Create a concrete implementation for testing
        class TestAdapter(BaseAdapter):
            adapter_name = "test"
            
            def fetch(self, *args, **kwargs):
                pass
            
            def get_schema(self, *args, **kwargs):
                pass
        
        return TestAdapter(host="test.example.com")
    
    @pytest.fixture
    def sample_table(self):
        """Create a sample PyArrow table for testing."""
        return pa.table({
            "id": [1, 2, 3, 4, 5],
            "status": ["open", "open", "closed", "open", "closed"],
            "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
            "count": [10, 20, 15, 30, 25],
        })
    
    @pytest.fixture
    def large_table(self):
        """Create a large table for performance warning tests."""
        n = 6000  # Above warning threshold
        return pa.table({
            "id": list(range(n)),
            "value": [float(i) for i in range(n)],
        })

    # --- Streaming Aggregate Tests ---
    
    def test_streaming_aggregate_count_star(self, adapter, sample_table):
        """Test COUNT(*) without GROUP BY."""
        aggregates = [MockAggregate(func="COUNT", column="*")]
        result = adapter._streaming_aggregate(sample_table, aggregates)
        
        assert len(result) == 1
        assert result.column("COUNT(*)")[0].as_py() == 5
    
    def test_streaming_aggregate_count_column(self, adapter, sample_table):
        """Test COUNT(column) - counts non-null values."""
        aggregates = [MockAggregate(func="COUNT", column="status")]
        result = adapter._streaming_aggregate(sample_table, aggregates)
        
        assert result.column("COUNT(status)")[0].as_py() == 5
    
    def test_streaming_aggregate_sum(self, adapter, sample_table):
        """Test SUM aggregation."""
        aggregates = [MockAggregate(func="SUM", column="amount")]
        result = adapter._streaming_aggregate(sample_table, aggregates)
        
        assert result.column("SUM(amount)")[0].as_py() == 1000.0
    
    def test_streaming_aggregate_avg(self, adapter, sample_table):
        """Test AVG aggregation."""
        aggregates = [MockAggregate(func="AVG", column="amount")]
        result = adapter._streaming_aggregate(sample_table, aggregates)
        
        assert result.column("AVG(amount)")[0].as_py() == 200.0
    
    def test_streaming_aggregate_min_max(self, adapter, sample_table):
        """Test MIN and MAX aggregations."""
        aggregates = [
            MockAggregate(func="MIN", column="amount", alias="min_amount"),
            MockAggregate(func="MAX", column="amount", alias="max_amount"),
        ]
        result = adapter._streaming_aggregate(sample_table, aggregates)
        
        assert result.column("min_amount")[0].as_py() == 100.0
        assert result.column("max_amount")[0].as_py() == 300.0
    
    def test_streaming_aggregate_multiple(self, adapter, sample_table):
        """Test multiple aggregations at once."""
        aggregates = [
            MockAggregate(func="COUNT", column="*", alias="cnt"),
            MockAggregate(func="SUM", column="amount", alias="total"),
            MockAggregate(func="AVG", column="amount", alias="average"),
        ]
        result = adapter._streaming_aggregate(sample_table, aggregates)
        
        assert result.column("cnt")[0].as_py() == 5
        assert result.column("total")[0].as_py() == 1000.0
        assert result.column("average")[0].as_py() == 200.0
    
    def test_streaming_aggregate_missing_column(self, adapter, sample_table):
        """Test aggregation on non-existent column returns None."""
        aggregates = [MockAggregate(func="SUM", column="nonexistent")]
        result = adapter._streaming_aggregate(sample_table, aggregates)
        
        assert result.column("SUM(nonexistent)")[0].as_py() is None

    # --- GROUP BY Aggregate Tests ---
    
    def test_aggregate_with_groupby_count(self, adapter, sample_table):
        """Test COUNT with GROUP BY."""
        aggregates = [MockAggregate(func="COUNT", column="*", alias="cnt")]
        result = adapter._aggregate_with_groupby(sample_table, ["status"], aggregates)
        
        # Should have 2 groups: open (3), closed (2)
        assert len(result) == 2
        result_dict = {row["status"]: row["cnt"] for row in result.to_pylist()}
        assert result_dict["open"] == 3
        assert result_dict["closed"] == 2
    
    def test_aggregate_with_groupby_sum(self, adapter, sample_table):
        """Test SUM with GROUP BY."""
        aggregates = [MockAggregate(func="SUM", column="amount", alias="total")]
        result = adapter._aggregate_with_groupby(sample_table, ["status"], aggregates)
        
        result_dict = {row["status"]: row["total"] for row in result.to_pylist()}
        assert result_dict["open"] == 600.0  # 100 + 200 + 300
        assert result_dict["closed"] == 400.0  # 150 + 250
    
    def test_aggregate_with_groupby_multiple_aggs(self, adapter, sample_table):
        """Test multiple aggregations with GROUP BY."""
        aggregates = [
            MockAggregate(func="COUNT", column="*", alias="cnt"),
            MockAggregate(func="SUM", column="amount", alias="total"),
            MockAggregate(func="AVG", column="amount", alias="avg"),
        ]
        result = adapter._aggregate_with_groupby(sample_table, ["status"], aggregates)
        
        result_list = result.to_pylist()
        open_row = next(r for r in result_list if r["status"] == "open")
        
        assert open_row["cnt"] == 3
        assert open_row["total"] == 600.0
        assert open_row["avg"] == 200.0

    # --- Approximate Aggregate Tests ---
    
    def test_approximate_aggregate_small_table(self, adapter, sample_table):
        """Approximate aggregation on small table returns exact results."""
        aggregates = [MockAggregate(func="COUNT", column="*", alias="cnt")]
        result = adapter._compute_approximate_aggregates(
            sample_table, None, aggregates, sample_size=100
        )
        
        # Should be exact since table is smaller than sample size
        assert result.column("cnt")[0].as_py() == 5
    
    def test_approximate_aggregate_large_table(self, adapter):
        """Approximate aggregation on large table uses sampling."""
        # Create a large table
        n = 50000
        large_table = pa.table({
            "id": list(range(n)),
            "value": [1.0] * n,  # All 1s for easy verification
        })
        
        aggregates = [
            MockAggregate(func="COUNT", column="*", alias="cnt"),
            MockAggregate(func="SUM", column="value", alias="total"),
        ]
        
        result = adapter._compute_approximate_aggregates(
            large_table, None, aggregates, sample_size=1000
        )
        
        # COUNT should be approximately n (with sampling adjustment)
        count = result.column("cnt")[0].as_py()
        assert 40000 < count < 60000  # Within reasonable range
        
        # SUM should also be approximately n
        total = result.column("total")[0].as_py()
        assert 40000 < total < 60000

    # --- Empty Table Tests ---
    
    def test_aggregation_empty_table(self, adapter):
        """Test aggregation on empty table."""
        empty_table = pa.table({"id": [], "value": []})
        aggregates = [MockAggregate(func="COUNT", column="*", alias="cnt")]
        
        result = adapter._compute_client_side_aggregates(empty_table, None, aggregates)
        
        assert len(result) == 1  # Standard SQL: SELECT COUNT(*) FROM empty returns 0 (1 row)
        assert result.column("cnt")[0].as_py() == 0

    # --- Performance Warning Tests ---
    
    def test_performance_warning_logged(self, adapter, large_table, caplog):
        """Test that performance warning is logged for large tables."""
        aggregates = [MockAggregate(func="COUNT", column="*")]
        
        with caplog.at_level(logging.WARNING):
            adapter._compute_client_side_aggregates(large_table, None, aggregates)
        
        assert "Client-side aggregation on 6,000 rows" in caplog.text
        assert "may be slow" in caplog.text


# --- Smart COUNT Optimization Tests ---

class TestSmartCountOptimization:
    """Tests for Smart COUNT optimization in SaaS adapters."""
    
    # --- HubSpot Tests ---
    
    @respx.mock
    @pytest.mark.asyncio
    async def test_hubspot_smart_count(self):
        """Test HubSpot uses 'total' field for COUNT(*)."""
        adapter = HubSpotAdapter(
            host="api.hubapi.com",
            access_token="test_token"
        )
        
        # Mock the Search API response
        respx.post("https://api.hubapi.com/crm/v3/objects/contacts/search").mock(
            return_value=httpx.Response(200, json={
                "total": 12345,
                "results": [{"id": "1", "properties": {}}]
            })
        )
        
        aggregates = [MockAggregate(func="COUNT", column="*", alias="cnt")]
        result = await adapter.fetch_async("contacts", aggregates=aggregates)
        
        assert len(result) == 1
        assert result.column("cnt")[0].as_py() == 12345
    
    def test_hubspot_is_simple_count_detection(self):
        """Test _is_simple_count correctly identifies simple COUNT queries."""
        adapter = HubSpotAdapter(host="api.hubapi.com", access_token="test")
        
        # Should be simple count
        assert adapter._is_simple_count(
            [MockAggregate(func="COUNT", column="*")], None
        ) is True
        
        assert adapter._is_simple_count(
            [MockAggregate(func="count", column="*")], None  # lowercase
        ) is True
        
        # Should NOT be simple count
        assert adapter._is_simple_count(
            [MockAggregate(func="COUNT", column="*")], ["status"]  # has GROUP BY
        ) is False
        
        assert adapter._is_simple_count(
            [MockAggregate(func="SUM", column="amount")], None  # not COUNT
        ) is False
        
        assert adapter._is_simple_count(
            [
                MockAggregate(func="COUNT", column="*"),
                MockAggregate(func="SUM", column="amount")
            ], None  # multiple aggregates
        ) is False

    # --- Shopify Tests ---
    
    @respx.mock
    @pytest.mark.asyncio
    async def test_shopify_smart_count(self):
        """Test Shopify uses /count.json endpoint for COUNT(*)."""
        adapter = ShopifyAdapter(
            host="test-shop.myshopify.com",
            api_key="test_token"
        )
        
        # Mock the count endpoint
        respx.get("https://test-shop.myshopify.com/admin/api/2024-01/products/count.json").mock(
            return_value=httpx.Response(200, json={"count": 9876})
        )
        
        aggregates = [MockAggregate(func="COUNT", column="*", alias="product_count")]
        result = await adapter.fetch_async("products", aggregates=aggregates)
        
        assert len(result) == 1
        assert result.column("product_count")[0].as_py() == 9876

    # --- Zendesk Tests ---
    
    @respx.mock
    @pytest.mark.asyncio
    async def test_zendesk_smart_count(self):
        """Test Zendesk uses 'count' field from Search API."""
        adapter = ZendeskAdapter(
            host="test.zendesk.com",
            email="test@example.com",
            api_token="test_token"
        )
        
        # Mock the Search API response
        respx.get("https://test.zendesk.com/api/v2/search.json").mock(
            return_value=httpx.Response(200, json={
                "count": 5432,
                "results": [{"id": 1}]
            })
        )
        
        aggregates = [MockAggregate(func="COUNT", column="*", alias="ticket_count")]
        result = await adapter.fetch_async("tickets", aggregates=aggregates)
        
        assert len(result) == 1
        assert result.column("ticket_count")[0].as_py() == 5432

    # --- Stripe Tests ---
    
    @respx.mock
    @pytest.mark.asyncio
    async def test_stripe_smart_count_with_search(self):
        """Test Stripe uses total_count from Search API when available."""
        adapter = StripeAdapter(
            host="api.stripe.com",
            api_key="sk_test_xxx"
        )
        
        # Mock the Search API response (searchable resource with predicates)
        respx.get("https://api.stripe.com/v1/customers/search").mock(
            return_value=httpx.Response(200, json={
                "total_count": 1111,
                "data": [{"id": "cus_xxx"}]
            })
        )
        
        predicates = [Predicate(column="email", operator="=", value="test@example.com")]
        aggregates = [MockAggregate(func="COUNT", column="*", alias="customer_count")]
        result = await adapter.fetch_async("customers", predicates=predicates, aggregates=aggregates)
        
        assert len(result) == 1
        assert result.column("customer_count")[0].as_py() == 1111


# --- Integration Tests ---

class TestAggregationIntegration:
    """Integration tests for aggregation with full adapter flow."""
    
    @respx.mock
    @pytest.mark.asyncio
    async def test_hubspot_client_side_aggregation_with_groupby(self):
        """Test client-side aggregation with GROUP BY (no Smart COUNT)."""
        adapter = HubSpotAdapter(
            host="api.hubapi.com",
            access_token="test_token"
        )
        
        # Mock search response with data for GROUP BY
        respx.post("https://api.hubapi.com/crm/v3/objects/deals/search").mock(
            return_value=httpx.Response(200, json={
                "total": 4,
                "results": [
                    {"id": "1", "properties": {"dealstage": "won", "amount": "1000"}},
                    {"id": "2", "properties": {"dealstage": "won", "amount": "2000"}},
                    {"id": "3", "properties": {"dealstage": "lost", "amount": "500"}},
                    {"id": "4", "properties": {"dealstage": "lost", "amount": "750"}},
                ]
            })
        )
        
        aggregates = [MockAggregate(func="COUNT", column="*", alias="cnt")]
        result = await adapter.fetch_async(
            "deals",
            group_by=["dealstage"],
            aggregates=aggregates
        )
        
        # Should have 2 groups with correct counts
        assert len(result) == 2
        result_dict = {row["dealstage"]: row["cnt"] for row in result.to_pylist()}
        assert result_dict["won"] == 2
        assert result_dict["lost"] == 2


# --- Edge Case Tests ---

class TestAggregationEdgeCases:
    """Tests for edge cases in aggregation."""
    
    @pytest.fixture
    def adapter(self):
        class TestAdapter(BaseAdapter):
            adapter_name = "test"
            def fetch(self, *args, **kwargs): pass
            def get_schema(self, *args, **kwargs): pass
        return TestAdapter(host="test.example.com")
    
    def test_aggregation_with_null_values(self, adapter):
        """Test COUNT correctly handles NULL values."""
        table = pa.table({
            "id": [1, 2, 3, 4, 5],
            "value": [100.0, None, 200.0, None, 300.0],
        })
        
        aggregates = [
            MockAggregate(func="COUNT", column="value", alias="non_null_count"),
            MockAggregate(func="SUM", column="value", alias="total"),
        ]
        
        result = adapter._streaming_aggregate(table, aggregates)
        
        # COUNT(value) should only count non-null values
        assert result.column("non_null_count")[0].as_py() == 3
        # SUM should ignore nulls
        assert result.column("total")[0].as_py() == 600.0
    
    def test_custom_alias(self, adapter):
        """Test that custom aliases are correctly applied."""
        table = pa.table({"amount": [100, 200, 300]})
        
        aggregates = [
            MockAggregate(func="SUM", column="amount", alias="my_custom_total"),
        ]
        
        result = adapter._streaming_aggregate(table, aggregates)
        
        assert "my_custom_total" in result.column_names
        assert result.column("my_custom_total")[0].as_py() == 600
    
    def test_no_aggregates_returns_original(self, adapter):
        """Test that empty aggregates returns original table."""
        table = pa.table({"id": [1, 2, 3]})
        
        result = adapter._compute_client_side_aggregates(table, None, None)
        
        assert result.equals(table)
    
    def test_supports_aggregation_flag(self):
        """Test that adapters correctly set supports_aggregation flag."""
        hubspot = HubSpotAdapter(host="api.hubapi.com", access_token="test")
        shopify = ShopifyAdapter(host="test.myshopify.com", api_key="test")
        zendesk = ZendeskAdapter(host="test.zendesk.com", email="test@test.com", api_token="test")
        stripe = StripeAdapter(host="api.stripe.com", api_key="test")
        
        assert hubspot.supports_aggregation is True
        assert shopify.supports_aggregation is True
        assert zendesk.supports_aggregation is True
        assert stripe.supports_aggregation is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
