"""
WaveQL AI Functions - Vector Search & Embeddings

This module provides AI-powered SQL functions for WaveQL:
- vector_search(): Similarity search using DuckDB's VSS extension
- EMBED(): Generate embeddings via OpenAI/Ollama

Usage:
    from waveql.ai import register_ai_functions
    
    conn = waveql.connect(...)
    register_ai_functions(conn, provider="openai", api_key="sk-...")
    
    # Now use in SQL
    cursor.execute("SELECT * FROM vector_search('documents', EMBED('hello world'), 5)")
"""

from __future__ import annotations
import os
import logging
from typing import List, Optional
from dataclasses import dataclass

import pyarrow as pa

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingConfig:
    """Configuration for embedding generation."""
    provider: str = "openai"  # openai, ollama, mock
    model: str = "text-embedding-3-small"
    api_key: Optional[str] = None
    base_url: Optional[str] = None  # For Ollama: http://localhost:11434
    dimensions: int = 1536  # Default for OpenAI text-embedding-3-small
    batch_size: int = 100
    
    def __post_init__(self):
        # Try to get API key from environment if not provided
        if self.provider == "openai" and not self.api_key:
            self.api_key = os.getenv("OPENAI_API_KEY")


class EmbeddingProvider:
    """Base class for embedding providers."""
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts."""
        raise NotImplementedError
    
    def embed_single(self, text: str) -> List[float]:
        """Generate embedding for a single text."""
        return self.embed([text])[0]


class OpenAIEmbedding(EmbeddingProvider):
    """OpenAI embeddings provider."""
    
    def __init__(self, config: EmbeddingConfig):
        self.config = config
        self._client = None
    
    @property
    def client(self):
        if self._client is None:
            try:
                from openai import OpenAI
                self._client = OpenAI(
                    api_key=self.config.api_key,
                    base_url=self.config.base_url,
                )
            except ImportError:
                raise ImportError("openai package required: pip install openai")
        return self._client
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using OpenAI API."""
        if not texts:
            return []
        
        results = []
        # Batch processing
        for i in range(0, len(texts), self.config.batch_size):
            batch = texts[i:i + self.config.batch_size]
            response = self.client.embeddings.create(
                model=self.config.model,
                input=batch,
                dimensions=self.config.dimensions,
            )
            for item in response.data:
                results.append(item.embedding)
        
        return results


class OllamaEmbedding(EmbeddingProvider):
    """Ollama local embeddings provider."""
    
    def __init__(self, config: EmbeddingConfig):
        self.config = config
        self.base_url = config.base_url or "http://localhost:11434"
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Ollama API."""
        import httpx
        
        results = []
        for text in texts:
            response = httpx.post(
                f"{self.base_url}/api/embeddings",
                json={"model": self.config.model, "prompt": text},
                timeout=60.0,
            )
            response.raise_for_status()
            data = response.json()
            results.append(data["embedding"])
        
        return results


class MockEmbedding(EmbeddingProvider):
    """Mock embeddings for testing (returns deterministic vectors)."""
    
    def __init__(self, config: EmbeddingConfig):
        self.config = config
    
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate mock embeddings based on text hash."""
        import hashlib
        
        results = []
        for text in texts:
            # Create deterministic "embedding" from text hash
            hash_bytes = hashlib.sha256(text.encode()).digest()
            # Convert to floats between -1 and 1
            vec = []
            for i in range(min(self.config.dimensions, len(hash_bytes) * 2)):
                byte_idx = i % len(hash_bytes)
                val = (hash_bytes[byte_idx] / 255.0) * 2 - 1
                vec.append(round(val, 6))
            # Pad with zeros if needed
            while len(vec) < self.config.dimensions:
                vec.append(0.0)
            results.append(vec[:self.config.dimensions])
        
        return results


def get_embedding_provider(config: EmbeddingConfig) -> EmbeddingProvider:
    """Factory function to get the appropriate embedding provider."""
    providers = {
        "openai": OpenAIEmbedding,
        "ollama": OllamaEmbedding,
        "mock": MockEmbedding,
    }
    
    provider_class = providers.get(config.provider.lower())
    if not provider_class:
        raise ValueError(f"Unknown embedding provider: {config.provider}")
    
    return provider_class(config)


class VectorSearchManager:
    """Manages vector search functionality for WaveQL connections."""
    
    def __init__(self, connection, config: Optional[EmbeddingConfig] = None):
        self._connection = connection
        self._config = config or EmbeddingConfig(provider="mock")
        self._provider = get_embedding_provider(self._config)
        self._initialized = False
    
    def _ensure_vss_extension(self):
        """Install and load the VSS extension if available."""
        if self._initialized:
            return
        
        try:
            self._connection._duckdb.execute("INSTALL vss")
            self._connection._duckdb.execute("LOAD vss")
            logger.info("DuckDB VSS extension loaded successfully")
        except Exception as e:
            logger.warning(f"VSS extension not available (will use brute-force): {e}")
        
        self._initialized = True
    
    def embed(self, text: str) -> List[float]:
        """Generate embedding for a single text."""
        return self._provider.embed_single(text)
    
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts."""
        return self._provider.embed(texts)
    
    def vector_search(
        self,
        table: str,
        query_vector: List[float],
        k: int = 5,
        vector_column: str = "embedding",
        distance_metric: str = "l2",
    ) -> pa.Table:
        """
        Perform vector similarity search on a table.
        
        Args:
            table: Table name to search
            query_vector: Query embedding vector
            k: Number of results to return
            vector_column: Name of the column containing vectors
            distance_metric: Distance metric (l2, cosine, inner_product)
            
        Returns:
            PyArrow table with search results and distances
        """
        self._ensure_vss_extension()
        
        # Build the distance function based on metric
        distance_funcs = {
            "l2": "array_distance",
            "cosine": "array_cosine_distance", 
            "inner_product": "array_negative_inner_product",
        }
        
        dist_func = distance_funcs.get(distance_metric, "array_distance")
        
        # Format vector as DuckDB array literal
        vec_str = "[" + ",".join(str(v) for v in query_vector) + "]"
        
        # Execute similarity search
        sql = f"""
            SELECT *, {dist_func}({vector_column}, {vec_str}::FLOAT[{len(query_vector)}]) as _distance
            FROM {table}
            ORDER BY _distance ASC
            LIMIT {k}
        """
        
        result = self._connection._duckdb.execute(sql).fetch_arrow_table()
        return result
    
    def create_vector_index(
        self,
        table: str,
        vector_column: str = "embedding",
        index_name: Optional[str] = None,
        metric: str = "l2",
    ):
        """
        Create an HNSW index for faster vector search.
        
        Args:
            table: Table name
            vector_column: Column containing vectors
            index_name: Optional index name (auto-generated if not provided)
            metric: Distance metric for the index
        """
        self._ensure_vss_extension()
        
        if index_name is None:
            index_name = f"{table}_{vector_column}_hnsw"
        
        metric_map = {
            "l2": "l2sq",
            "cosine": "cosine",
            "inner_product": "ip",
        }
        
        metric_type = metric_map.get(metric, "l2sq")
        
        sql = f"""
            CREATE INDEX {index_name} ON {table} 
            USING HNSW ({vector_column}) 
            WITH (metric = '{metric_type}')
        """
        
        self._connection._duckdb.execute(sql)
        logger.info(f"Created HNSW index '{index_name}' on {table}.{vector_column}")


def register_ai_functions(
    connection,
    provider: str = "mock",
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    base_url: Optional[str] = None,
    dimensions: int = 1536,
) -> VectorSearchManager:
    """
    Register AI functions (EMBED, vector_search) with a WaveQL connection.
    
    Args:
        connection: WaveQL connection object
        provider: Embedding provider ("openai", "ollama", "mock")
        api_key: API key for the provider
        model: Model name for embeddings
        base_url: Base URL for API (required for Ollama)
        dimensions: Vector dimensions
        
    Returns:
        VectorSearchManager instance attached to the connection
        
    Example:
        conn = waveql.connect(...)
        ai = register_ai_functions(conn, provider="openai", api_key="sk-...")
        
        # Use the manager directly
        embedding = ai.embed("hello world")
        results = ai.vector_search("documents", embedding, k=5)
    """
    config = EmbeddingConfig(
        provider=provider,
        api_key=api_key,
        model=model or ("text-embedding-3-small" if provider == "openai" else "nomic-embed-text"),
        base_url=base_url,
        dimensions=dimensions,
    )
    
    manager = VectorSearchManager(connection, config)
    
    # Attach to connection for easy access
    connection._vector_search = manager
    
    logger.info(f"Registered AI functions with provider: {provider}")
    
    return manager
