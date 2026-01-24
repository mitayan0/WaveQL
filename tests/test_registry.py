"""Tests for adapter registry."""

import pytest
from waveql.adapters.registry import register_adapter, get_adapter, list_adapters
from unittest.mock import MagicMock


class TestAdapterRegistry:
    def test_register_and_get(self):
        mock_cls = MagicMock()
        register_adapter("test_custom_adapter", mock_cls)
        result = get_adapter("test_custom_adapter")
        assert result is mock_cls

    def test_list_adapters(self):
        adapters = list_adapters()
        assert isinstance(adapters, list)
        assert len(adapters) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
