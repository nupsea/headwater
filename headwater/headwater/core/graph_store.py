"""Kuzu graph store -- relationship pattern discovery and graph queries.

Loads tables as nodes and FK relationships as edges. Runs pattern
queries to discover conformed dimensions, star schemas, chains,
and nullable FK warnings at build time.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any

import kuzu

logger = logging.getLogger(__name__)


class GraphStore:
    """Kuzu-backed embedded graph database for relationship analysis."""

    def __init__(self, store_path: str | Path) -> None:
        self._path = Path(store_path)
        self._db: kuzu.Database | None = None
        self._conn: kuzu.Connection | None = None

    @property
    def db(self) -> kuzu.Database:
        if self._db is None:
            # Kuzu creates the directory itself; only ensure parent exists
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._db = kuzu.Database(str(self._path))
            logger.info("Connected to Kuzu graph store at %s", self._path)
        return self._db

    @property
    def conn(self) -> kuzu.Connection:
        if self._conn is None:
            self._conn = kuzu.Connection(self.db)
        return self._conn

    def init_schema(self) -> None:
        """Create the graph schema (node and edge tables)."""
        conn = self.conn
        # Create node table for database tables
        conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS DataTable(
                name STRING,
                row_count INT64,
                domain STRING,
                description STRING,
                PRIMARY KEY (name)
            )
        """)
        # Create edge table for FK relationships
        conn.execute("""
            CREATE REL TABLE IF NOT EXISTS FK(
                FROM DataTable TO DataTable,
                from_column STRING,
                to_column STRING,
                rel_type STRING,
                confidence DOUBLE,
                ref_integrity DOUBLE,
                nullable BOOLEAN
            )
        """)
        logger.info("Graph schema initialized")

    def load_tables(self, tables: list[dict[str, Any]]) -> int:
        """Load table metadata as nodes.

        Each dict should have: name, row_count, domain (optional), description (optional).
        Returns number of nodes loaded.
        """
        conn = self.conn
        count = 0
        for t in tables:
            try:
                conn.execute(
                    "MERGE (n:DataTable {name: $name}) "
                    "SET n.row_count = $row_count, "
                    "n.domain = $domain, "
                    "n.description = $description",
                    parameters={
                        "name": t["name"],
                        "row_count": t.get("row_count", 0),
                        "domain": t.get("domain", ""),
                        "description": t.get("description", ""),
                    },
                )
                count += 1
            except Exception as e:
                logger.warning("Failed to load table node %s: %s", t["name"], e)
        logger.info("Loaded %d table nodes", count)
        return count

    def load_relationships(self, relationships: list[dict[str, Any]]) -> int:
        """Load FK relationships as edges.

        Each dict should have: from_table, from_column, to_table, to_column,
        rel_type, confidence, ref_integrity.
        Returns number of edges loaded.
        """
        conn = self.conn
        count = 0
        for r in relationships:
            nullable = r.get("ref_integrity", 1.0) < 0.5
            try:
                conn.execute(
                    "MATCH (a:DataTable {name: $from_table}), "
                    "(b:DataTable {name: $to_table}) "
                    "CREATE (a)-[:FK {"
                    "from_column: $from_column, "
                    "to_column: $to_column, "
                    "rel_type: $rel_type, "
                    "confidence: $confidence, "
                    "ref_integrity: $ref_integrity, "
                    "nullable: $nullable"
                    "}]->(b)",
                    parameters={
                        "from_table": r["from_table"],
                        "to_table": r["to_table"],
                        "from_column": r["from_column"],
                        "to_column": r["to_column"],
                        "rel_type": r.get("rel_type", "many_to_one"),
                        "confidence": r.get("confidence", 0.5),
                        "ref_integrity": r.get("ref_integrity", 1.0),
                        "nullable": nullable,
                    },
                )
                count += 1
            except Exception as e:
                logger.warning(
                    "Failed to load relationship %s.%s -> %s.%s: %s",
                    r["from_table"],
                    r["from_column"],
                    r["to_table"],
                    r["to_column"],
                    e,
                )
        logger.info("Loaded %d relationship edges", count)
        return count

    def find_conformed_dimensions(self, min_connections: int = 2) -> list[dict[str, Any]]:
        """Find tables that serve as dimensions for multiple fact tables.

        A conformed dimension is a table that has incoming FK edges from
        at least min_connections other tables.

        Returns list of dicts: {name, connection_count, connected_tables}.
        """
        result = self.conn.execute(
            "MATCH (fact:DataTable)-[r:FK]->(dim:DataTable) "
            "WITH dim.name AS dim_name, collect(fact.name) AS facts, count(*) AS cnt "
            "WHERE cnt >= $min_conn "
            "RETURN dim_name, facts, cnt "
            "ORDER BY cnt DESC",
            parameters={"min_conn": min_connections},
        )
        rows = []
        while result.has_next():
            row = result.get_next()
            rows.append(
                {
                    "name": row[0],
                    "connection_count": row[2],
                    "connected_tables": row[1],
                }
            )
        logger.info(
            "Conformed dimensions: %d tables with >= %d connections",
            len(rows),
            min_connections,
        )
        return rows

    def find_star_schemas(self) -> list[dict[str, Any]]:
        """Identify star schema patterns: hub tables connected to 3+ tables.

        Returns list of dicts: {hub, spokes, spoke_count}.
        """
        result = self.conn.execute(
            "MATCH (spoke:DataTable)-[:FK]->(hub:DataTable) "
            "WITH hub.name AS hub_name, collect(spoke.name) AS spokes, count(*) AS cnt "
            "WHERE cnt >= 3 "
            "RETURN hub_name, spokes, cnt "
            "ORDER BY cnt DESC",
        )
        rows = []
        while result.has_next():
            row = result.get_next()
            rows.append(
                {
                    "hub": row[0],
                    "spokes": row[1],
                    "spoke_count": row[2],
                }
            )
        logger.info("Star schemas: %d hub tables found", len(rows))
        return rows

    def find_chains(self, max_hops: int = 4) -> list[dict[str, Any]]:
        """Find FK chains (paths) of 2+ hops.

        Returns list of dicts: {path, hop_count}.
        """
        result = self.conn.execute(
            "MATCH p = (a:DataTable)-[:FK*2..4]->(b:DataTable) "
            "RETURN nodes(p), length(p) AS hops "
            "ORDER BY hops DESC",
        )
        rows = []
        seen = set()
        while result.has_next():
            row = result.get_next()
            nodes = row[0]
            path = [n["name"] for n in nodes]
            path_key = " -> ".join(path)
            if path_key not in seen:
                seen.add(path_key)
                rows.append(
                    {
                        "path": path,
                        "hop_count": row[1],
                    }
                )
        logger.info("FK chains: %d unique paths of 2+ hops", len(rows))
        return rows

    def find_nullable_fk_warnings(self, threshold: float = 0.5) -> list[dict[str, Any]]:
        """Find FK relationships with low referential integrity (nullable FKs).

        Returns list of dicts: {from_table, from_column, to_table, to_column,
        ref_integrity, nullable}.
        """
        result = self.conn.execute(
            "MATCH (a:DataTable)-[r:FK]->(b:DataTable) "
            "WHERE r.ref_integrity < $threshold "
            "RETURN a.name, r.from_column, b.name, r.to_column, "
            "r.ref_integrity, r.nullable "
            "ORDER BY r.ref_integrity ASC",
            parameters={"threshold": threshold},
        )
        rows = []
        while result.has_next():
            row = result.get_next()
            rows.append(
                {
                    "from_table": row[0],
                    "from_column": row[1],
                    "to_table": row[2],
                    "to_column": row[3],
                    "ref_integrity": row[4],
                    "nullable": row[5],
                }
            )
        if rows:
            logger.warning(
                "Nullable FK warnings: %d relationships with integrity < %.0f%%",
                len(rows),
                threshold * 100,
            )
        return rows

    def get_join_path(
        self,
        from_table: str,
        to_table: str,
        max_hops: int = 3,
    ) -> list[dict[str, Any]] | None:
        """Find the shortest FK path between two tables.

        Returns list of edge dicts [{from_table, from_column, to_table, to_column,
        nullable}] or None if no path exists.
        """
        try:
            result = self.conn.execute(
                "MATCH p = shortestPath("
                "(a:DataTable {name: $from_t})-[:FK*1..3]->(b:DataTable {name: $to_t})"
                ") "
                "RETURN rels(p)",
                parameters={"from_t": from_table, "to_t": to_table},
            )
            if not result.has_next():
                # Try reverse direction
                result = self.conn.execute(
                    "MATCH p = shortestPath("
                    "(a:DataTable {name: $to_t})-[:FK*1..3]->(b:DataTable {name: $from_t})"
                    ") "
                    "RETURN rels(p)",
                    parameters={"from_t": from_table, "to_t": to_table},
                )
                if not result.has_next():
                    return None

            row = result.get_next()
            edges = row[0]
            path = []
            for edge in edges:
                path.append(
                    {
                        "from_column": edge.get("from_column", ""),
                        "to_column": edge.get("to_column", ""),
                        "nullable": edge.get("nullable", False),
                    }
                )
            return path
        except Exception as e:
            logger.warning("Failed to find join path %s -> %s: %s", from_table, to_table, e)
            return None

    def get_graph_data(self) -> dict[str, Any]:
        """Return full graph data for visualization.

        Returns dict with 'nodes' and 'edges' lists suitable for D3/force-graph.
        """
        nodes = []
        result = self.conn.execute(
            "MATCH (n:DataTable) RETURN n.name, n.row_count, n.domain, n.description",
        )
        while result.has_next():
            row = result.get_next()
            nodes.append(
                {
                    "id": row[0],
                    "row_count": row[1],
                    "domain": row[2],
                    "description": row[3],
                }
            )

        edges = []
        result = self.conn.execute(
            "MATCH (a:DataTable)-[r:FK]->(b:DataTable) "
            "RETURN a.name, b.name, r.from_column, r.to_column, "
            "r.rel_type, r.confidence, r.ref_integrity, r.nullable",
        )
        while result.has_next():
            row = result.get_next()
            edges.append(
                {
                    "source": row[0],
                    "target": row[1],
                    "from_column": row[2],
                    "to_column": row[3],
                    "rel_type": row[4],
                    "confidence": row[5],
                    "ref_integrity": row[6],
                    "nullable": row[7],
                }
            )

        logger.debug("Graph data: %d nodes, %d edges", len(nodes), len(edges))
        return {"nodes": nodes, "edges": edges}

    def clear(self) -> None:
        """Drop all data and recreate schema."""
        logger.info("Clearing graph store at %s", self._path)
        self.close()
        if self._path.exists():
            if self._path.is_dir():
                shutil.rmtree(self._path)
            else:
                self._path.unlink()
        self._db = None
        self._conn = None
        self.init_schema()

    def close(self) -> None:
        """Close the Kuzu connection and database."""
        self._conn = None
        self._db = None
