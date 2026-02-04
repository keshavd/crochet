"""DDL and data operations available inside migration upgrade/downgrade functions."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Operation:
    """A single recorded operation for audit purposes."""

    op_type: str
    details: dict[str, Any]


class MigrationContext:
    """Context object passed to upgrade() and downgrade() functions.

    Wraps Neo4j operations and records everything for auditability.
    When *dry_run* is ``True`` operations are recorded but not executed.
    """

    def __init__(self, driver: Any | None = None, dry_run: bool = False) -> None:
        self._driver = driver
        self._dry_run = dry_run
        self.operations: list[Operation] = []
        self._batch_id: str | None = None

    # ------------------------------------------------------------------
    # Constraints
    # ------------------------------------------------------------------

    def add_unique_constraint(self, label: str, property_name: str) -> None:
        """CREATE CONSTRAINT … REQUIRE (n.prop) IS UNIQUE."""
        constraint_name = f"crochet_uniq_{label}_{property_name}"
        cypher = (
            f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE n.{property_name} IS UNIQUE"
        )
        self._record_and_run("add_unique_constraint", {
            "label": label, "property": property_name, "cypher": cypher,
        })

    def drop_unique_constraint(self, label: str, property_name: str) -> None:
        constraint_name = f"crochet_uniq_{label}_{property_name}"
        cypher = f"DROP CONSTRAINT {constraint_name} IF EXISTS"
        self._record_and_run("drop_unique_constraint", {
            "label": label, "property": property_name, "cypher": cypher,
        })

    def add_node_property_existence_constraint(
        self, label: str, property_name: str
    ) -> None:
        constraint_name = f"crochet_exists_{label}_{property_name}"
        cypher = (
            f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE n.{property_name} IS NOT NULL"
        )
        self._record_and_run("add_existence_constraint", {
            "label": label, "property": property_name, "cypher": cypher,
        })

    def drop_node_property_existence_constraint(
        self, label: str, property_name: str
    ) -> None:
        constraint_name = f"crochet_exists_{label}_{property_name}"
        cypher = f"DROP CONSTRAINT {constraint_name} IF EXISTS"
        self._record_and_run("drop_existence_constraint", {
            "label": label, "property": property_name, "cypher": cypher,
        })

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------

    def add_index(self, label: str, property_name: str) -> None:
        index_name = f"crochet_idx_{label}_{property_name}"
        cypher = (
            f"CREATE INDEX {index_name} IF NOT EXISTS "
            f"FOR (n:{label}) ON (n.{property_name})"
        )
        self._record_and_run("add_index", {
            "label": label, "property": property_name, "cypher": cypher,
        })

    def drop_index(self, label: str, property_name: str) -> None:
        index_name = f"crochet_idx_{label}_{property_name}"
        cypher = f"DROP INDEX {index_name} IF EXISTS"
        self._record_and_run("drop_index", {
            "label": label, "property": property_name, "cypher": cypher,
        })

    # ------------------------------------------------------------------
    # Labels / Relationship types
    # ------------------------------------------------------------------

    def rename_label(self, old_label: str, new_label: str) -> None:
        cypher = (
            f"MATCH (n:{old_label}) "
            f"SET n:{new_label} REMOVE n:{old_label}"
        )
        self._record_and_run("rename_label", {
            "old_label": old_label, "new_label": new_label, "cypher": cypher,
        })

    def rename_relationship_type(self, old_type: str, new_type: str) -> None:
        cypher = (
            f"MATCH (a)-[r:{old_type}]->(b) "
            f"CREATE (a)-[r2:{new_type}]->(b) "
            f"SET r2 = properties(r) "
            f"DELETE r"
        )
        self._record_and_run("rename_relationship_type", {
            "old_type": old_type, "new_type": new_type, "cypher": cypher,
        })

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    def add_node_property(
        self, label: str, property_name: str, default: Any = None
    ) -> None:
        if default is not None:
            cypher = f"MATCH (n:{label}) SET n.{property_name} = $default"
            params = {"default": default}
        else:
            cypher = None
            params = None
        self._record_and_run("add_node_property", {
            "label": label, "property": property_name,
            "default": default, "cypher": cypher,
        }, params=params)

    def remove_node_property(self, label: str, property_name: str) -> None:
        cypher = f"MATCH (n:{label}) REMOVE n.{property_name}"
        self._record_and_run("remove_node_property", {
            "label": label, "property": property_name, "cypher": cypher,
        })

    def rename_node_property(
        self, label: str, old_name: str, new_name: str
    ) -> None:
        cypher = (
            f"MATCH (n:{label}) "
            f"SET n.{new_name} = n.{old_name} "
            f"REMOVE n.{old_name}"
        )
        self._record_and_run("rename_node_property", {
            "label": label, "old_name": old_name, "new_name": new_name,
            "cypher": cypher,
        })

    # ------------------------------------------------------------------
    # Raw Cypher (escape hatch)
    # ------------------------------------------------------------------

    def run_cypher(self, cypher: str, params: dict | None = None) -> Any:
        """Execute arbitrary Cypher — use sparingly."""
        return self._record_and_run("run_cypher", {
            "cypher": cypher, "params": params,
        }, params=params, cypher_override=cypher)

    # ------------------------------------------------------------------
    # Data ingest helpers
    # ------------------------------------------------------------------

    def begin_batch(self, batch_id: str | None = None) -> str:
        """Start a data-ingest batch. Returns the batch ID."""
        self._batch_id = batch_id or uuid.uuid4().hex[:12]
        self._record_and_run("begin_batch", {"batch_id": self._batch_id})
        return self._batch_id

    @property
    def batch_id(self) -> str | None:
        return self._batch_id

    def create_nodes(
        self, label: str, data: list[dict[str, Any]]
    ) -> int:
        """Create nodes from a list of property dictionaries.

        Each node is tagged with ``_crochet_batch`` for rollback.
        """
        if not data:
            return 0
        batch = self._batch_id or "untracked"
        cypher = (
            f"UNWIND $rows AS row "
            f"CREATE (n:{label}) SET n = row, n._crochet_batch = $batch"
        )
        self._record_and_run("create_nodes", {
            "label": label, "count": len(data), "cypher": cypher,
        }, params={"rows": data, "batch": batch}, cypher_override=cypher)
        return len(data)

    def create_relationships(
        self,
        source_label: str,
        target_label: str,
        rel_type: str,
        data: list[dict[str, Any]],
        source_key: str = "source_id",
        target_key: str = "target_id",
        properties_key: str = "properties",
    ) -> int:
        """Create relationships from structured data rows.

        Each row must contain *source_key* and *target_key* values, and
        optionally a *properties_key* dict.
        """
        if not data:
            return 0
        batch = self._batch_id or "untracked"
        cypher = (
            f"UNWIND $rows AS row "
            f"MATCH (a:{source_label} {{id: row.{source_key}}}) "
            f"MATCH (b:{target_label} {{id: row.{target_key}}}) "
            f"CREATE (a)-[r:{rel_type}]->(b) "
            f"SET r = row.{properties_key}, r._crochet_batch = $batch"
        )
        self._record_and_run("create_relationships", {
            "source_label": source_label, "target_label": target_label,
            "rel_type": rel_type, "count": len(data), "cypher": cypher,
        }, params={"rows": data, "batch": batch}, cypher_override=cypher)
        return len(data)

    # ------------------------------------------------------------------
    # Upsert (incremental / merge) helpers
    # ------------------------------------------------------------------

    def upsert_nodes(
        self,
        label: str,
        data: list[dict[str, Any]],
        merge_keys: list[str],
    ) -> int:
        """Create or update nodes using MERGE on *merge_keys*.

        Existing nodes matched by *merge_keys* are updated; new nodes are
        created.  All nodes are tagged with ``_crochet_batch``.

        Parameters
        ----------
        label:
            Node label.
        data:
            Rows of property dictionaries.
        merge_keys:
            Property names to match on (the node identity).
        """
        if not data or not merge_keys:
            return 0
        batch = self._batch_id or "untracked"

        merge_clause = ", ".join(f"{k}: row.{k}" for k in merge_keys)
        cypher = (
            f"UNWIND $rows AS row "
            f"MERGE (n:{label} {{{merge_clause}}}) "
            f"SET n += row, n._crochet_batch = $batch"
        )
        self._record_and_run("upsert_nodes", {
            "label": label, "count": len(data), "merge_keys": merge_keys,
            "cypher": cypher,
        }, params={"rows": data, "batch": batch}, cypher_override=cypher)
        return len(data)

    def upsert_relationships(
        self,
        source_label: str,
        target_label: str,
        rel_type: str,
        data: list[dict[str, Any]],
        source_key: str = "source_id",
        target_key: str = "target_id",
        properties_key: str = "properties",
    ) -> int:
        """Create or update relationships using MERGE.

        Matches source and target nodes by ``id``, then merges the
        relationship.  Properties under *properties_key* are set on the
        relationship.

        Parameters
        ----------
        source_label, target_label:
            Labels for the source and target nodes.
        rel_type:
            Relationship type.
        data:
            Rows containing *source_key*, *target_key*, and optionally
            *properties_key*.
        """
        if not data:
            return 0
        batch = self._batch_id or "untracked"
        cypher = (
            f"UNWIND $rows AS row "
            f"MATCH (a:{source_label} {{id: row.{source_key}}}) "
            f"MATCH (b:{target_label} {{id: row.{target_key}}}) "
            f"MERGE (a)-[r:{rel_type}]->(b) "
            f"SET r += row.{properties_key}, r._crochet_batch = $batch"
        )
        self._record_and_run("upsert_relationships", {
            "source_label": source_label, "target_label": target_label,
            "rel_type": rel_type, "count": len(data), "cypher": cypher,
        }, params={"rows": data, "batch": batch}, cypher_override=cypher)
        return len(data)

    # ------------------------------------------------------------------
    # Bulk / chunked helpers
    # ------------------------------------------------------------------

    def bulk_create_nodes(
        self,
        label: str,
        data: list[dict[str, Any]],
        chunk_size: int = 5000,
        use_call_in_transactions: bool = False,
    ) -> int:
        """Create nodes in chunked batches for large datasets.

        Parameters
        ----------
        label:
            Node label.
        data:
            Rows of property dictionaries.
        chunk_size:
            Number of rows per Cypher UNWIND batch.  Default 5000.
        use_call_in_transactions:
            When ``True``, use ``CALL {} IN TRANSACTIONS OF N ROWS``
            (Neo4j 4.4+) for server-side batching.
        """
        if not data:
            return 0
        batch = self._batch_id or "untracked"

        if use_call_in_transactions:
            cypher = (
                f"UNWIND $rows AS row "
                f"CALL {{ WITH row "
                f"CREATE (n:{label}) SET n = row, n._crochet_batch = $batch "
                f"}} IN TRANSACTIONS OF {chunk_size} ROWS"
            )
            self._record_and_run("bulk_create_nodes", {
                "label": label, "count": len(data), "chunk_size": chunk_size,
                "method": "CALL_IN_TRANSACTIONS", "cypher": cypher,
            }, params={"rows": data, "batch": batch}, cypher_override=cypher)
        else:
            total = 0
            for i in range(0, len(data), chunk_size):
                chunk = data[i : i + chunk_size]
                cypher = (
                    f"UNWIND $rows AS row "
                    f"CREATE (n:{label}) SET n = row, n._crochet_batch = $batch"
                )
                self._record_and_run("bulk_create_nodes", {
                    "label": label, "count": len(chunk),
                    "chunk_index": i // chunk_size,
                    "chunk_size": chunk_size,
                    "method": "client_chunked", "cypher": cypher,
                }, params={"rows": chunk, "batch": batch}, cypher_override=cypher)
                total += len(chunk)
        return len(data)

    def bulk_upsert_nodes(
        self,
        label: str,
        data: list[dict[str, Any]],
        merge_keys: list[str],
        chunk_size: int = 5000,
        use_call_in_transactions: bool = False,
    ) -> int:
        """Upsert nodes in chunked batches for large datasets.

        Parameters
        ----------
        label:
            Node label.
        data:
            Rows of property dictionaries.
        merge_keys:
            Property names to match on (the node identity).
        chunk_size:
            Number of rows per batch.  Default 5000.
        use_call_in_transactions:
            When ``True``, use ``CALL {} IN TRANSACTIONS OF N ROWS``.
        """
        if not data or not merge_keys:
            return 0
        batch = self._batch_id or "untracked"
        merge_clause = ", ".join(f"{k}: row.{k}" for k in merge_keys)

        if use_call_in_transactions:
            cypher = (
                f"UNWIND $rows AS row "
                f"CALL {{ WITH row "
                f"MERGE (n:{label} {{{merge_clause}}}) "
                f"SET n += row, n._crochet_batch = $batch "
                f"}} IN TRANSACTIONS OF {chunk_size} ROWS"
            )
            self._record_and_run("bulk_upsert_nodes", {
                "label": label, "count": len(data), "merge_keys": merge_keys,
                "chunk_size": chunk_size, "method": "CALL_IN_TRANSACTIONS",
                "cypher": cypher,
            }, params={"rows": data, "batch": batch}, cypher_override=cypher)
        else:
            for i in range(0, len(data), chunk_size):
                chunk = data[i : i + chunk_size]
                cypher = (
                    f"UNWIND $rows AS row "
                    f"MERGE (n:{label} {{{merge_clause}}}) "
                    f"SET n += row, n._crochet_batch = $batch"
                )
                self._record_and_run("bulk_upsert_nodes", {
                    "label": label, "count": len(chunk),
                    "merge_keys": merge_keys,
                    "chunk_index": i // chunk_size,
                    "chunk_size": chunk_size,
                    "method": "client_chunked", "cypher": cypher,
                }, params={"rows": chunk, "batch": batch}, cypher_override=cypher)
        return len(data)

    def bulk_create_relationships(
        self,
        source_label: str,
        target_label: str,
        rel_type: str,
        data: list[dict[str, Any]],
        chunk_size: int = 5000,
        source_key: str = "source_id",
        target_key: str = "target_id",
        properties_key: str = "properties",
        use_call_in_transactions: bool = False,
    ) -> int:
        """Create relationships in chunked batches for large datasets."""
        if not data:
            return 0
        batch = self._batch_id or "untracked"

        if use_call_in_transactions:
            cypher = (
                f"UNWIND $rows AS row "
                f"CALL {{ WITH row "
                f"MATCH (a:{source_label} {{id: row.{source_key}}}) "
                f"MATCH (b:{target_label} {{id: row.{target_key}}}) "
                f"CREATE (a)-[r:{rel_type}]->(b) "
                f"SET r = row.{properties_key}, r._crochet_batch = $batch "
                f"}} IN TRANSACTIONS OF {chunk_size} ROWS"
            )
            self._record_and_run("bulk_create_relationships", {
                "source_label": source_label, "target_label": target_label,
                "rel_type": rel_type, "count": len(data),
                "chunk_size": chunk_size, "method": "CALL_IN_TRANSACTIONS",
                "cypher": cypher,
            }, params={"rows": data, "batch": batch}, cypher_override=cypher)
        else:
            for i in range(0, len(data), chunk_size):
                chunk = data[i : i + chunk_size]
                cypher = (
                    f"UNWIND $rows AS row "
                    f"MATCH (a:{source_label} {{id: row.{source_key}}}) "
                    f"MATCH (b:{target_label} {{id: row.{target_key}}}) "
                    f"CREATE (a)-[r:{rel_type}]->(b) "
                    f"SET r = row.{properties_key}, r._crochet_batch = $batch"
                )
                self._record_and_run("bulk_create_relationships", {
                    "source_label": source_label, "target_label": target_label,
                    "rel_type": rel_type, "count": len(chunk),
                    "chunk_index": i // chunk_size,
                    "chunk_size": chunk_size,
                    "method": "client_chunked", "cypher": cypher,
                }, params={"rows": chunk, "batch": batch}, cypher_override=cypher)
        return len(data)

    # ------------------------------------------------------------------
    # Deletion
    # ------------------------------------------------------------------

    def delete_nodes_by_batch(self, label: str, batch_id: str) -> None:
        """Delete all nodes of a label that belong to a batch."""
        cypher = (
            f"MATCH (n:{label} {{_crochet_batch: $batch}}) DETACH DELETE n"
        )
        self._record_and_run("delete_nodes_by_batch", {
            "label": label, "batch_id": batch_id, "cypher": cypher,
        }, params={"batch": batch_id}, cypher_override=cypher)

    def delete_relationships_by_batch(self, rel_type: str, batch_id: str) -> None:
        """Delete all relationships of a type that belong to a batch."""
        cypher = (
            f"MATCH ()-[r:{rel_type} {{_crochet_batch: $batch}}]-() DELETE r"
        )
        self._record_and_run("delete_relationships_by_batch", {
            "rel_type": rel_type, "batch_id": batch_id, "cypher": cypher,
        }, params={"batch": batch_id}, cypher_override=cypher)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _record_and_run(
        self,
        op_type: str,
        details: dict[str, Any],
        params: dict | None = None,
        cypher_override: str | None = None,
    ) -> Any:
        self.operations.append(Operation(op_type=op_type, details=details))
        if self._dry_run or self._driver is None:
            return None
        cypher = cypher_override or details.get("cypher")
        if cypher:
            with self._driver.session() as session:
                result = session.run(cypher, **(params or {}))
                return result.consume()
        return None
