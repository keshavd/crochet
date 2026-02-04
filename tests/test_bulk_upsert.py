"""Tests for upsert and bulk import operations on MigrationContext."""

from __future__ import annotations

import pytest

from crochet.migrations.operations import MigrationContext


# ---------------------------------------------------------------------------
# Upsert nodes
# ---------------------------------------------------------------------------


class TestUpsertNodes:
    def test_basic_upsert(self):
        ctx = MigrationContext(dry_run=True)
        ctx.begin_batch("batch_u1")
        count = ctx.upsert_nodes(
            "Person",
            [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
            merge_keys=["name"],
        )
        assert count == 2
        # begin_batch + upsert_nodes
        assert len(ctx.operations) == 2
        op = ctx.operations[1]
        assert op.op_type == "upsert_nodes"
        assert "MERGE" in op.details["cypher"]
        assert "name: row.name" in op.details["cypher"]

    def test_multiple_merge_keys(self):
        ctx = MigrationContext(dry_run=True)
        count = ctx.upsert_nodes(
            "Gene",
            [{"symbol": "TP53", "species": "human", "name": "Tumor Protein P53"}],
            merge_keys=["symbol", "species"],
        )
        assert count == 1
        cypher = ctx.operations[0].details["cypher"]
        assert "symbol: row.symbol" in cypher
        assert "species: row.species" in cypher

    def test_empty_data(self):
        ctx = MigrationContext(dry_run=True)
        assert ctx.upsert_nodes("X", [], merge_keys=["id"]) == 0
        assert len(ctx.operations) == 0

    def test_empty_merge_keys(self):
        ctx = MigrationContext(dry_run=True)
        assert ctx.upsert_nodes("X", [{"id": 1}], merge_keys=[]) == 0

    def test_batch_tag_in_cypher(self):
        ctx = MigrationContext(dry_run=True)
        ctx.begin_batch("my_batch")
        ctx.upsert_nodes("Person", [{"name": "Alice"}], merge_keys=["name"])
        cypher = ctx.operations[1].details["cypher"]
        assert "_crochet_batch" in cypher


# ---------------------------------------------------------------------------
# Upsert relationships
# ---------------------------------------------------------------------------


class TestUpsertRelationships:
    def test_basic_upsert(self):
        ctx = MigrationContext(dry_run=True)
        ctx.begin_batch("batch_r1")
        count = ctx.upsert_relationships(
            "Person", "Person", "KNOWS",
            [{"source_id": "1", "target_id": "2", "properties": {"since": 2020}}],
        )
        assert count == 1
        op = ctx.operations[1]
        assert op.op_type == "upsert_relationships"
        assert "MERGE" in op.details["cypher"]

    def test_empty_data(self):
        ctx = MigrationContext(dry_run=True)
        assert ctx.upsert_relationships("A", "B", "R", []) == 0


# ---------------------------------------------------------------------------
# Bulk create nodes
# ---------------------------------------------------------------------------


class TestBulkCreateNodes:
    def test_client_chunked(self):
        ctx = MigrationContext(dry_run=True)
        ctx.begin_batch("bulk_1")
        data = [{"id": i, "name": f"item_{i}"} for i in range(12)]
        count = ctx.bulk_create_nodes("Item", data, chunk_size=5)
        assert count == 12
        # begin_batch + 3 chunk ops (5+5+2)
        assert len(ctx.operations) == 4
        # Verify chunk sizes in operation details
        assert ctx.operations[1].details["count"] == 5
        assert ctx.operations[2].details["count"] == 5
        assert ctx.operations[3].details["count"] == 2
        assert ctx.operations[1].details["method"] == "client_chunked"

    def test_call_in_transactions(self):
        ctx = MigrationContext(dry_run=True)
        ctx.begin_batch("bulk_2")
        data = [{"id": i} for i in range(20)]
        count = ctx.bulk_create_nodes(
            "Item", data, chunk_size=10, use_call_in_transactions=True
        )
        assert count == 20
        # begin_batch + 1 CALL IN TRANSACTIONS op
        assert len(ctx.operations) == 2
        op = ctx.operations[1]
        assert "CALL_IN_TRANSACTIONS" in op.details["method"]
        assert "IN TRANSACTIONS OF 10 ROWS" in op.details["cypher"]

    def test_empty_data(self):
        ctx = MigrationContext(dry_run=True)
        assert ctx.bulk_create_nodes("X", []) == 0

    def test_single_chunk(self):
        ctx = MigrationContext(dry_run=True)
        data = [{"id": 1}, {"id": 2}]
        count = ctx.bulk_create_nodes("X", data, chunk_size=100)
        assert count == 2
        assert len(ctx.operations) == 1  # Only 1 chunk needed


# ---------------------------------------------------------------------------
# Bulk upsert nodes
# ---------------------------------------------------------------------------


class TestBulkUpsertNodes:
    def test_client_chunked(self):
        ctx = MigrationContext(dry_run=True)
        ctx.begin_batch("bu_1")
        data = [{"name": f"p_{i}", "age": i} for i in range(7)]
        count = ctx.bulk_upsert_nodes(
            "Person", data, merge_keys=["name"], chunk_size=3
        )
        assert count == 7
        # begin_batch + 3 chunk ops (3+3+1)
        assert len(ctx.operations) == 4
        # All should use MERGE
        for op in ctx.operations[1:]:
            assert "MERGE" in op.details["cypher"]
            assert op.details["method"] == "client_chunked"

    def test_call_in_transactions(self):
        ctx = MigrationContext(dry_run=True)
        data = [{"name": f"p_{i}"} for i in range(10)]
        count = ctx.bulk_upsert_nodes(
            "Person", data, merge_keys=["name"],
            chunk_size=5, use_call_in_transactions=True,
        )
        assert count == 10
        assert len(ctx.operations) == 1
        assert "MERGE" in ctx.operations[0].details["cypher"]
        assert "IN TRANSACTIONS OF 5 ROWS" in ctx.operations[0].details["cypher"]

    def test_empty_data(self):
        ctx = MigrationContext(dry_run=True)
        assert ctx.bulk_upsert_nodes("X", [], merge_keys=["id"]) == 0

    def test_empty_merge_keys(self):
        ctx = MigrationContext(dry_run=True)
        assert ctx.bulk_upsert_nodes("X", [{"id": 1}], merge_keys=[]) == 0


# ---------------------------------------------------------------------------
# Bulk create relationships
# ---------------------------------------------------------------------------


class TestBulkCreateRelationships:
    def test_client_chunked(self):
        ctx = MigrationContext(dry_run=True)
        ctx.begin_batch("br_1")
        data = [
            {"source_id": str(i), "target_id": str(i + 1), "properties": {}}
            for i in range(8)
        ]
        count = ctx.bulk_create_relationships(
            "Person", "Person", "KNOWS", data, chunk_size=3
        )
        assert count == 8
        # begin_batch + 3 chunk ops (3+3+2)
        assert len(ctx.operations) == 4

    def test_call_in_transactions(self):
        ctx = MigrationContext(dry_run=True)
        data = [
            {"source_id": "1", "target_id": "2", "properties": {}},
        ]
        count = ctx.bulk_create_relationships(
            "A", "B", "R", data,
            chunk_size=100, use_call_in_transactions=True,
        )
        assert count == 1
        assert "IN TRANSACTIONS" in ctx.operations[0].details["cypher"]

    def test_empty_data(self):
        ctx = MigrationContext(dry_run=True)
        assert ctx.bulk_create_relationships("A", "B", "R", []) == 0


# ---------------------------------------------------------------------------
# Integration: batch tag carried through all operations
# ---------------------------------------------------------------------------


class TestBatchTagIntegration:
    def test_all_ops_carry_batch(self):
        ctx = MigrationContext(dry_run=True)
        ctx.begin_batch("integration_batch")

        ctx.create_nodes("A", [{"x": 1}])
        ctx.upsert_nodes("B", [{"x": 1}], merge_keys=["x"])
        ctx.bulk_create_nodes("C", [{"x": 1}], chunk_size=100)
        ctx.bulk_upsert_nodes("D", [{"x": 1}], merge_keys=["x"], chunk_size=100)

        # All data ops should mention _crochet_batch
        for op in ctx.operations[1:]:  # skip begin_batch
            assert "_crochet_batch" in op.details["cypher"], (
                f"Op {op.op_type} missing _crochet_batch in Cypher"
            )
