"""Tests for the SQLite ledger."""

from __future__ import annotations

import pytest

from crochet.errors import LedgerError
from crochet.ledger.sqlite import Ledger


class TestLedgerMigrations:
    def test_record_and_get(self, ledger: Ledger):
        ledger.record_migration("001_init", None, "initial", "hash_a")
        applied = ledger.get_applied_migrations()
        assert len(applied) == 1
        assert applied[0].revision_id == "001_init"
        assert applied[0].parent_id is None
        assert applied[0].schema_hash == "hash_a"

    def test_head(self, ledger: Ledger):
        assert ledger.get_head() is None
        ledger.record_migration("001", None, "first", "h1")
        ledger.record_migration("002", "001", "second", "h2")
        head = ledger.get_head()
        assert head is not None
        assert head.revision_id == "002"

    def test_is_applied(self, ledger: Ledger):
        assert not ledger.is_applied("001")
        ledger.record_migration("001", None, "first", "h1")
        assert ledger.is_applied("001")

    def test_remove_migration(self, ledger: Ledger):
        ledger.record_migration("001", None, "first", "h1")
        ledger.remove_migration("001")
        assert not ledger.is_applied("001")

    def test_duplicate_raises(self, ledger: Ledger):
        ledger.record_migration("001", None, "first", "h1")
        with pytest.raises(LedgerError, match="already recorded"):
            ledger.record_migration("001", None, "first", "h1")

    def test_chain_ordering(self, ledger: Ledger):
        ledger.record_migration("001", None, "a", "h1")
        ledger.record_migration("002", "001", "b", "h2")
        ledger.record_migration("003", "002", "c", "h3")
        applied = ledger.get_applied_migrations()
        ids = [m.revision_id for m in applied]
        assert ids == ["001", "002", "003"]


class TestLedgerBatches:
    def test_record_batch(self, ledger: Ledger):
        batch = ledger.record_batch("batch_001", source_file="data.csv")
        assert batch.batch_id == "batch_001"
        assert batch.source_file == "data.csv"

    def test_get_batches(self, ledger: Ledger):
        ledger.record_batch("b1")
        ledger.record_batch("b2")
        batches = ledger.get_batches()
        assert len(batches) == 2

    def test_get_batches_by_migration(self, ledger: Ledger):
        ledger.record_migration("m1", None, "first", "h")
        ledger.record_batch("b1", migration_id="m1")
        ledger.record_batch("b2")
        assert len(ledger.get_batches("m1")) == 1

    def test_remove_batch(self, ledger: Ledger):
        ledger.record_batch("b1")
        ledger.remove_batch("b1")
        assert len(ledger.get_batches()) == 0


class TestLedgerSnapshots:
    def test_store_and_get(self, ledger: Ledger):
        ledger.store_snapshot("hash_abc", '{"nodes": []}')
        result = ledger.get_snapshot("hash_abc")
        assert result == '{"nodes": []}'

    def test_get_missing(self, ledger: Ledger):
        assert ledger.get_snapshot("nonexistent") is None


class TestLedgerIntegrity:
    def test_verify_chain_clean(self, ledger: Ledger):
        ledger.record_migration("001", None, "a", "h1")
        ledger.record_migration("002", "001", "b", "h2")
        assert ledger.verify_chain() == []

    def test_verify_chain_broken(self, ledger: Ledger):
        ledger.record_migration("002", "001", "b", "h2")  # 001 doesn't exist
        issues = ledger.verify_chain()
        assert len(issues) == 1
        assert "001" in issues[0]


class TestLedgerContextManager:
    def test_context_manager(self, config):
        with Ledger(config.ledger_file) as led:
            led.record_migration("001", None, "test", "h1")
            assert led.is_applied("001")
