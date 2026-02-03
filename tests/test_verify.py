"""Tests for verification logic."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from crochet.config import CrochetConfig
from crochet.ledger.sqlite import Ledger
from crochet.verify import verify_project


def _write_migration(mig_dir: Path, rev_id: str, parent: str | None = None) -> None:
    parent_repr = repr(parent)
    content = textwrap.dedent(f"""\
        from crochet.migrations.operations import MigrationContext
        revision_id = "{rev_id}"
        parent_id = {parent_repr}
        schema_hash = "hash_{rev_id}"
        rollback_safe = True
        def upgrade(ctx: MigrationContext) -> None:
            pass
        def downgrade(ctx: MigrationContext) -> None:
            pass
    """)
    (mig_dir / f"{rev_id}.py").write_text(content)


class TestVerify:
    def test_clean_project(self, config: CrochetConfig, ledger: Ledger):
        """An empty project with no migrations should pass."""
        report = verify_project(config, ledger)
        assert report.passed

    def test_pending_migrations_fail(self, config: CrochetConfig, ledger: Ledger):
        _write_migration(config.migrations_dir, "0001_init")
        report = verify_project(config, ledger)
        assert not report.passed
        summaries = report.summary()
        assert "Pending" in summaries

    def test_all_applied_pass(self, config: CrochetConfig, ledger: Ledger):
        _write_migration(config.migrations_dir, "0001_init")
        ledger.record_migration("0001_init", None, "init", "hash_0001_init")
        report = verify_project(config, ledger)
        assert report.passed

    def test_missing_file_fails(self, config: CrochetConfig, ledger: Ledger):
        """Ledger references a migration whose file doesn't exist."""
        ledger.record_migration("0001_ghost", None, "ghost", "h")
        report = verify_project(config, ledger)
        assert not report.passed

    def test_broken_chain_fails(self, config: CrochetConfig, ledger: Ledger):
        _write_migration(config.migrations_dir, "0002_second", parent="0001_first")
        ledger.record_migration("0002_second", "0001_first", "second", "hash_0002_second")
        report = verify_project(config, ledger)
        # Chain is broken because 0001_first doesn't exist in ledger
        assert not report.passed

    def test_summary_output(self, config: CrochetConfig, ledger: Ledger):
        report = verify_project(config, ledger)
        summary = report.summary()
        assert "PASS" in summary or "FAIL" in summary
