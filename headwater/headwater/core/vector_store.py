"""LanceDB vector store -- catalog embedding index and semantic search.

Stores catalog entries (metrics, dimensions, entities) as embeddings
for semantic similarity search during query decomposition.
Uses sentence-transformers for encoding.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import lancedb
import pyarrow as pa

logger = logging.getLogger(__name__)

# Default embedding model -- lightweight, CPU-friendly, 384-dim
_DEFAULT_MODEL = "all-MiniLM-L6-v2"

# Embedding dimension for all-MiniLM-L6-v2
_EMBEDDING_DIM = 384

# Table name in LanceDB
_CATALOG_TABLE = "catalog_entries"

# Singleton embedding model (lazy-loaded)
_model = None


def _get_model():
    """Lazy-load the sentence-transformers model."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer(_DEFAULT_MODEL)
        logger.info("Loaded embedding model: %s", _DEFAULT_MODEL)
    return _model


def encode_text(text: str) -> list[float]:
    """Encode a single text string to an embedding vector."""
    model = _get_model()
    embedding = model.encode(text, convert_to_numpy=True)
    return embedding.tolist()


def encode_texts(texts: list[str]) -> list[list[float]]:
    """Encode multiple texts to embedding vectors (batched for efficiency)."""
    if not texts:
        return []
    model = _get_model()
    embeddings = model.encode(texts, convert_to_numpy=True, batch_size=64)
    return embeddings.tolist()


class VectorStore:
    """LanceDB-backed vector store for catalog semantic search."""

    def __init__(self, store_path: str | Path) -> None:
        self._path = str(store_path)
        self._db: lancedb.DBConnection | None = None

    @property
    def db(self) -> lancedb.DBConnection:
        if self._db is None:
            self._db = lancedb.connect(self._path)
            logger.info("Connected to LanceDB at %s", self._path)
        return self._db

    def _ensure_table(self) -> lancedb.table.Table:
        """Get or create the catalog_entries table."""
        try:
            return self.db.open_table(_CATALOG_TABLE)
        except Exception:
            # Create empty table with schema
            schema = pa.schema(
                [
                    pa.field("id", pa.string()),
                    pa.field("project_id", pa.string()),
                    pa.field("entry_type", pa.string()),  # metric, dimension, entity
                    pa.field("name", pa.string()),
                    pa.field("display_name", pa.string()),
                    pa.field("text", pa.string()),  # Full text used for embedding
                    pa.field("vector", pa.list_(pa.float32(), _EMBEDDING_DIM)),
                ]
            )
            table = self.db.create_table(_CATALOG_TABLE, schema=schema)
            logger.info("Created LanceDB table: %s", _CATALOG_TABLE)
            return table

    def index_entries(
        self,
        project_id: str,
        entries: list[dict[str, Any]],
    ) -> int:
        """Index catalog entries with embeddings.

        Each entry dict must have: id, entry_type, name, display_name, text.
        The 'text' field is used to generate the embedding.

        Returns the number of entries indexed.
        """
        if not entries:
            return 0

        # Clear existing entries for this project
        self.clear_project(project_id)

        texts = [e["text"] for e in entries]
        embeddings = encode_texts(texts)

        rows = []
        for entry, vector in zip(entries, embeddings, strict=True):
            rows.append(
                {
                    "id": entry["id"],
                    "project_id": project_id,
                    "entry_type": entry["entry_type"],
                    "name": entry["name"],
                    "display_name": entry["display_name"],
                    "text": entry["text"],
                    "vector": vector,
                }
            )

        table = self._ensure_table()
        table.add(rows)
        logger.info("Indexed %d catalog entries for project %s", len(rows), project_id)
        return len(rows)

    def search(
        self,
        query: str,
        project_id: str | None = None,
        entry_type: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Search catalog entries by semantic similarity.

        Args:
            query: Natural language query text.
            project_id: Optional filter by project.
            entry_type: Optional filter by type (metric, dimension, entity).
            limit: Maximum results to return.

        Returns:
            List of dicts with keys: id, entry_type, name, display_name, text,
            _distance (lower = more similar).
        """
        try:
            table = self.db.open_table(_CATALOG_TABLE)
        except Exception:
            return []

        query_vector = encode_text(query)

        search_builder = table.search(query_vector).limit(limit)

        # Apply filters
        filters = []
        if project_id:
            filters.append(f"project_id = '{project_id}'")
        if entry_type:
            filters.append(f"entry_type = '{entry_type}'")
        if filters:
            search_builder = search_builder.where(" AND ".join(filters))

        results = search_builder.to_list()

        return [
            {
                "id": r["id"],
                "entry_type": r["entry_type"],
                "name": r["name"],
                "display_name": r["display_name"],
                "text": r["text"],
                "_distance": r.get("_distance", 0.0),
            }
            for r in results
        ]

    def clear_project(self, project_id: str) -> None:
        """Remove all entries for a project."""
        try:
            table = self.db.open_table(_CATALOG_TABLE)
            table.delete(f"project_id = '{project_id}'")
        except Exception:
            pass  # Table doesn't exist yet

    def close(self) -> None:
        """Close the LanceDB connection."""
        self._db = None
