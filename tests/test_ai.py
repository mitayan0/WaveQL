
import pytest
import sys
from unittest.mock import MagicMock, patch, ANY
import pyarrow as pa
from waveql.ai import (
    EmbeddingConfig, MockEmbedding, OpenAIEmbedding, OllamaEmbedding,
    VectorSearchManager, get_embedding_provider, register_ai_functions
)

# --- Embedding Config & Provider Tests ---

def test_config_defaults():
    config = EmbeddingConfig()
    assert config.provider == "openai"
    assert config.dimensions == 1536

def test_mock_embedding():
    config = EmbeddingConfig(provider="mock", dimensions=10)
    provider = MockEmbedding(config)
    vectors = provider.embed(["hello", "world"])
    assert len(vectors) == 2
    assert len(vectors[0]) == 10
    # Deterministic check
    vectors2 = provider.embed(["hello"])
    assert vectors[0] == vectors2[0]

def test_openai_embedding():
    config = EmbeddingConfig(provider="openai", api_key="test_key")
    
    mock_openai = MagicMock()
    with patch.dict("sys.modules", {"openai": mock_openai}):
        # Reset OpenAIEmbedding client (if it was cached on class/instance?) 
        # OpenAIEmbedding caches client on instance.
        
        # Setup mock behavior
        mock_instance = mock_openai.OpenAI.return_value
        mock_response = MagicMock()
        mock_embedding_obj = MagicMock()
        mock_embedding_obj.embedding = [0.1] * 1536
        mock_response.data = [mock_embedding_obj, mock_embedding_obj]
        mock_instance.embeddings.create.return_value = mock_response
        
        provider = OpenAIEmbedding(config)
        vectors = provider.embed(["a", "b"])
        
        assert len(vectors) == 2
        assert vectors[0] == [0.1] * 1536
        mock_instance.embeddings.create.assert_called()

def test_ollama_embedding():
    config = EmbeddingConfig(provider="ollama", base_url="http://test")
    
    # httpx is a dep, so we can patch it directly or use sys.modules if lazy
    # It is imported lazily in method.
    with patch("httpx.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"embedding": [0.1, 0.2]}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp
        
        provider = OllamaEmbedding(config)
        vectors = provider.embed(["test"])
        
        assert vectors == [[0.1, 0.2]]
        mock_post.assert_called_with(
            "http://test/api/embeddings",
            json={"model": config.model, "prompt": "test"},
            timeout=60.0
        )

# --- Vector Search Manager Tests ---

@pytest.fixture
def mock_connection():
    conn = MagicMock()
    # Mock DuckDB connection
    conn._duckdb = MagicMock()
    return conn

@pytest.fixture
def vector_manager(mock_connection):
    config = EmbeddingConfig(provider="mock", dimensions=4)
    return VectorSearchManager(mock_connection, config)

def test_ensure_vss_extension(vector_manager):
    vector_manager._ensure_vss_extension()
    
    vector_manager._connection._duckdb.execute.assert_any_call("INSTALL vss")
    vector_manager._connection._duckdb.execute.assert_any_call("LOAD vss")
    
    # Second call should do nothing
    vector_manager._connection._duckdb.execute.reset_mock()
    vector_manager._ensure_vss_extension()
    vector_manager._connection._duckdb.execute.assert_not_called()

def test_vector_search(vector_manager):
    # Mock return
    mock_table = MagicMock()
    vector_manager._connection._duckdb.execute.return_value.fetch_arrow_table.return_value = mock_table
    
    result = vector_manager.vector_search("docs", [0.1, 0.2, 0.3, 0.4], k=10)
    
    assert result == mock_table
    
    # Verify SQL
    call_args = vector_manager._connection._duckdb.execute.call_args[0][0]
    assert "array_distance" in call_args # default l2
    assert "docs" in call_args
    assert "LIMIT 10" in call_args
    assert "[0.1, 0.2, 0.3, 0.4]" in call_args or "[0.1,0.2,0.3,0.4]" in call_args.replace(" ", "")

def test_create_vector_index(vector_manager):
    vector_manager.create_vector_index("docs", metric="cosine")
    
    call_args = vector_manager._connection._duckdb.execute.call_args_list[2][0][0] # 0,1 are INSTALL/LOAD
    assert "CREATE INDEX docs_embedding_hnsw" in call_args
    assert "USING HNSW (embedding)" in call_args
    assert "metric = 'cosine'" in call_args

def test_register_ai_functions(mock_connection):
    manager = register_ai_functions(mock_connection, provider="mock")
    
    assert isinstance(manager, VectorSearchManager)
    assert mock_connection._vector_search == manager
    assert manager._provider.__class__.__name__ == "MockEmbedding"

