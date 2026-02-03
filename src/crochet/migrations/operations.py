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
