"""Tests for materialized view manager."""

import pytest
from waveql.materialized_view.manager import MaterializedViewManager
from unittest.mock import MagicMock, patch


class TestMaterializedViewManager:
    def test_init(self):
        mock_conn = MagicMock()
        mock_conn._duck = MagicMock()
        with patch.object(MaterializedViewManager, '_register_existing_views'):
            mgr = MaterializedViewManager(mock_conn)
            assert mgr is not None

    def test_exists(self):
        mock_conn = MagicMock()
        mock_conn._duck = MagicMock()
        with patch.object(MaterializedViewManager, '_register_existing_views'):
            mgr = MaterializedViewManager(mock_conn)
            result = mgr.exists("nonexistent")
            assert result is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
