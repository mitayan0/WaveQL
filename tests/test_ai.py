"""
Tests for WaveQL AI Functions (Vector Search & Embeddings)
"""

import pytest
import pyarrow as pa

from waveql import connect, register_ai_functions, EmbeddingConfig, VectorSearchManager
from waveql.ai import (
    EmbeddingProvider,
    MockEmbedding,
    OpenAIEmbedding,
    OllamaEmbedding,
    get_embedding_provider,
)


class TestEmbeddingConfig:
    """Tests for EmbeddingConfig dataclass."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = EmbeddingConfig()
        
        assert config.provider == "openai"
        assert config.model == "text-embedding-3-small"
        assert config.dimensions == 1536
        assert config.batch_size == 100
    
    def test_custom_config(self):
        """Test custom configuration."""
        config = EmbeddingConfig(
            provider="ollama",
            model="nomic-embed-text",
            base_url="http://localhost:11434",
            dimensions=768,
        )
        
        assert config.provider == "ollama"
        assert config.model == "nomic-embed-text"
        assert config.base_url == "http://localhost:11434"
        assert config.dimensions == 768


class TestMockEmbedding:
    """Tests for MockEmbedding provider."""
    
    def test_embed_single(self):
        """Test embedding a single text."""
        config = EmbeddingConfig(provider="mock", dimensions=8)
        provider = MockEmbedding(config)
        
        embedding = provider.embed_single("hello world")
        
        assert isinstance(embedding, list)
        assert len(embedding) == 8
        assert all(isinstance(v, float) for v in embedding)
    
    def test_embed_batch(self):
        """Test embedding multiple texts."""
        config = EmbeddingConfig(provider="mock", dimensions=8)
        provider = MockEmbedding(config)
        
        texts = ["hello", "world", "test"]
        embeddings = provider.embed(texts)
        
        assert len(embeddings) == 3
        for emb in embeddings:
            assert len(emb) == 8
    
    def test_deterministic_embeddings(self):
        """Test that same text produces same embedding."""
        config = EmbeddingConfig(provider="mock", dimensions=8)
        provider = MockEmbedding(config)
        
        emb1 = provider.embed_single("test text")
        emb2 = provider.embed_single("test text")
        
        assert emb1 == emb2
    
    def test_different_texts_different_embeddings(self):
        """Test that different texts produce different embeddings."""
        config = EmbeddingConfig(provider="mock", dimensions=8)
        provider = MockEmbedding(config)
        
        emb1 = provider.embed_single("hello")
        emb2 = provider.embed_single("world")
        
        assert emb1 != emb2
    
    def test_empty_input(self):
        """Test embedding empty list."""
        config = EmbeddingConfig(provider="mock", dimensions=8)
        provider = MockEmbedding(config)
        
        embeddings = provider.embed([])
        
        assert embeddings == []


class TestGetEmbeddingProvider:
    """Tests for provider factory function."""
    
    def test_mock_provider(self):
        """Test getting mock provider."""
        config = EmbeddingConfig(provider="mock")
        provider = get_embedding_provider(config)
        
        assert isinstance(provider, MockEmbedding)
    
    def test_openai_provider(self):
        """Test getting OpenAI provider."""
        config = EmbeddingConfig(provider="openai", api_key="test-key")
        provider = get_embedding_provider(config)
        
        assert isinstance(provider, OpenAIEmbedding)
    
    def test_ollama_provider(self):
        """Test getting Ollama provider."""
        config = EmbeddingConfig(provider="ollama")
        provider = get_embedding_provider(config)
        
        assert isinstance(provider, OllamaEmbedding)
    
    def test_unknown_provider(self):
        """Test error for unknown provider."""
        config = EmbeddingConfig(provider="unknown")
        
        with pytest.raises(ValueError, match="Unknown embedding provider"):
            get_embedding_provider(config)
    
    def test_case_insensitive_provider(self):
        """Test provider name is case insensitive."""
        config = EmbeddingConfig(provider="MOCK")
        provider = get_embedding_provider(config)
        
        assert isinstance(provider, MockEmbedding)


class TestVectorSearchManager:
    """Tests for VectorSearchManager class."""
    
    @pytest.fixture
    def conn(self):
        """Create a test connection."""
        return connect()
    
    @pytest.fixture
    def manager(self, conn):
        """Create a VectorSearchManager with mock provider."""
        return VectorSearchManager(
            conn,
            EmbeddingConfig(provider="mock", dimensions=8)
        )
    
    def test_embed(self, manager):
        """Test embedding generation."""
        embedding = manager.embed("test text")
        
        assert isinstance(embedding, list)
        assert len(embedding) == 8
    
    def test_embed_batch(self, manager):
        """Test batch embedding."""
        embeddings = manager.embed_batch(["hello", "world"])
        
        assert len(embeddings) == 2
    
    def test_vector_search(self, conn, manager):
        """Test vector similarity search."""
        cursor = conn.cursor()
        
        # Create test table
        cursor.execute("""
            CREATE TABLE test_docs (
                id INTEGER,
                title VARCHAR,
                embedding FLOAT[8]
            )
        """)
        
        # Insert test data with embeddings
        docs = ["hello world", "machine learning", "database systems"]
        for i, doc in enumerate(docs):
            emb = manager.embed(doc)
            vec_str = "[" + ",".join(str(v) for v in emb) + "]"
            cursor.execute(f"""
                INSERT INTO test_docs VALUES ({i}, '{doc}', {vec_str}::FLOAT[8])
            """)
        
        # Search
        query_vec = manager.embed("machine learning AI")
        results = manager.vector_search(
            table="test_docs",
            query_vector=query_vec,
            k=2,
            vector_column="embedding",
        )
        
        assert isinstance(results, pa.Table)
        assert results.num_rows == 2
        assert "_distance" in results.column_names
        
        # Cleanup
        cursor.execute("DROP TABLE test_docs")
        conn.close()
    
    def test_vector_search_cosine(self, conn, manager):
        """Test vector search with cosine distance."""
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE cosine_test (
                id INTEGER,
                vec FLOAT[8]
            )
        """)
        
        # Insert normalized vectors
        emb = manager.embed("test")
        vec_str = "[" + ",".join(str(v) for v in emb) + "]"
        cursor.execute(f"INSERT INTO cosine_test VALUES (1, {vec_str}::FLOAT[8])")
        
        results = manager.vector_search(
            table="cosine_test",
            query_vector=emb,
            k=1,
            vector_column="vec",
            distance_metric="cosine",
        )
        
        assert results.num_rows == 1
        
        cursor.execute("DROP TABLE cosine_test")
        conn.close()


class TestRegisterAIFunctions:
    """Tests for register_ai_functions helper."""
    
    def test_register_with_mock(self):
        """Test registering AI functions with mock provider."""
        conn = connect()
        
        ai = register_ai_functions(conn, provider="mock", dimensions=8)
        
        assert isinstance(ai, VectorSearchManager)
        assert hasattr(conn, "_vector_search")
        assert conn._vector_search is ai
        
        conn.close()
    
    def test_register_with_openai(self):
        """Test registering with OpenAI provider."""
        conn = connect()
        
        ai = register_ai_functions(
            conn,
            provider="openai",
            api_key="test-key",
            model="text-embedding-3-small",
        )
        
        assert isinstance(ai, VectorSearchManager)
        assert ai._config.provider == "openai"
        assert ai._config.api_key == "test-key"
        
        conn.close()
    
    def test_register_with_ollama(self):
        """Test registering with Ollama provider."""
        conn = connect()
        
        ai = register_ai_functions(
            conn,
            provider="ollama",
            base_url="http://localhost:11434",
            model="nomic-embed-text",
        )
        
        assert isinstance(ai, VectorSearchManager)
        assert ai._config.provider == "ollama"
        assert ai._config.base_url == "http://localhost:11434"
        
        conn.close()


class TestIntegration:
    """Integration tests for complete AI workflow."""
    
    def test_end_to_end_vector_search(self):
        """Test complete workflow: embed -> store -> search."""
        conn = connect()
        cursor = conn.cursor()
        ai = register_ai_functions(conn, provider="mock", dimensions=8)
        
        # Create table
        cursor.execute("""
            CREATE TABLE documents (
                id INTEGER,
                content VARCHAR,
                embedding FLOAT[8]
            )
        """)
        
        # Insert documents with embeddings
        docs = [
            "Python programming tutorial",
            "Machine learning basics",
            "Web development guide",
            "Database optimization",
            "Cloud computing intro",
        ]
        
        for i, doc in enumerate(docs):
            emb = ai.embed(doc)
            vec_str = "[" + ",".join(str(v) for v in emb) + "]"
            cursor.execute(f"""
                INSERT INTO documents VALUES ({i}, '{doc}', {vec_str}::FLOAT[8])
            """)
        
        # Search for similar documents
        query = "ML and AI"
        query_vec = ai.embed(query)
        
        results = ai.vector_search(
            table="documents",
            query_vector=query_vec,
            k=3,
        )
        
        # Verify results
        assert results.num_rows == 3
        assert "_distance" in results.column_names
        
        # Results should be ordered by distance (ascending)
        distances = results.to_pydict()["_distance"]
        assert distances == sorted(distances)
        
        # Cleanup
        cursor.execute("DROP TABLE documents")
        conn.close()
    
    def test_direct_sql_array_distance(self):
        """Test using array_distance directly in SQL."""
        conn = connect()
        cursor = conn.cursor()
        ai = register_ai_functions(conn, provider="mock", dimensions=4)
        
        # Create simple table
        cursor.execute("""
            CREATE TABLE vecs (
                id INTEGER,
                vec FLOAT[4]
            )
        """)
        
        cursor.execute("INSERT INTO vecs VALUES (1, [1.0, 0.0, 0.0, 0.0]::FLOAT[4])")
        cursor.execute("INSERT INTO vecs VALUES (2, [0.0, 1.0, 0.0, 0.0]::FLOAT[4])")
        cursor.execute("INSERT INTO vecs VALUES (3, [1.0, 1.0, 0.0, 0.0]::FLOAT[4])")
        
        # Query with array_distance
        cursor.execute("""
            SELECT id, array_distance(vec, [1.0, 0.0, 0.0, 0.0]::FLOAT[4]) as dist
            FROM vecs
            ORDER BY dist ASC
        """)
        
        rows = cursor.fetchall()
        
        # First result should be exact match (distance = 0)
        assert rows[0]["id"] == 1
        assert rows[0]["dist"] == 0.0
        
        # Cleanup
        cursor.execute("DROP TABLE vecs")
        conn.close()
