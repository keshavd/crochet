"""Tests for migration engine, template, and operations."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from crochet.config import CrochetConfig
from crochet.errors import MigrationError, RollbackUnsafeError
from crochet.ir.schema import NodeIR, PropertyIR, SchemaSnapshot
from crochet.ir.hash import hash_snapshot
from crochet.ledger.sqlite import Ledger
from crochet.migrations.engine import MigrationEngine
from crochet.migrations.operations import MigrationContext
from crochet.migrations.template import (
    generate_revision_id,
    render_migration,
    slugify,
    write_migration_file,
)


# ======================================================================
# Template
# ======================================================================


class TestTemplate:
    def test_slugify(self):
        assert slugify("Add User Nodes") == "add_user_nodes"
        assert slugify("  Foo-Bar!Baz  ") == "foo_bar_baz"

    def test_generate_revision_id(self):
        rid = generate_revision_id(1, "initial setup")
        assert rid == "0001_initial_setup"

    def test_render_migration(self):
        content = render_migration(
            revision_id="0001_init",
            parent_id=None,
            description="Initial migration",
            schema_hash="abc123",
        )
        assert "revision_id = " in content
        assert "parent_id = None" in content
        assert "schema_hash = " in content
        assert "def upgrade" in content
        assert "def downgrade" in content

    def test_render_with_diff(self):
        content = render_migration(
            revision_id="0002_add_person",
            parent_id="0001_init",
            description="Add Person node",
            schema_hash="def456",
            diff_summary="+ Node 'Person' (kgid=person_v1)",
        )
        assert "Detected schema changes" in content
        assert "Person" in content

    def test_render_with_operations(self):
        from crochet.ir.diff import SchemaDiff, NodeChange, PropertyChange
        from crochet.ir.schema import NodeIR, PropertyIR
        
        # Mock a diff where a property is added and another is removed
        prop_added = PropertyIR(name="email", property_type="StringProperty", unique_index=True)
        prop_removed = PropertyIR(name="age", property_type="IntegerProperty")
        
        pc_added = PropertyChange(kind="added", property_name="email", new=prop_added)
        pc_removed = PropertyChange(kind="removed", property_name="age", old=prop_removed)
        
        node_old = NodeIR(kgid="p1", label="Person", class_name="Person", module_path="m", properties=(prop_removed,))
        node_new = NodeIR(kgid="p1", label="Person", class_name="Person", module_path="m", properties=(prop_added,))
        
        nc = NodeChange(kind="modified", kgid="p1", old=node_old, new=node_new, property_changes=[pc_added, pc_removed])
        diff = SchemaDiff(node_changes=[nc])
        
        content = render_migration(
            revision_id="0002_update_person",
            parent_id="0001_init",
            description="Update Person",
            schema_hash="def456",
            diff_summary=diff.summary(),
            diff=diff
        )
        
        # Verify upgrade contains operations
        assert 'ctx.add_node_property("Person", "email")' in content
        assert 'ctx.add_unique_constraint("Person", "email")' in content
        assert 'ctx.remove_node_property("Person", "age")' in content
        
        # Verify downgrade contains inverse operations
        assert 'ctx.remove_node_property("Person", "email")' in content
        assert 'ctx.drop_unique_constraint("Person", "email")' in content
        assert 'ctx.add_node_property("Person", "age")' in content

    def test_write_migration_file(self, tmp_path):
        mig_dir = tmp_path / "migrations"
        path = write_migration_file(mig_dir, "0001_init", "# test\n")
        assert path.exists()
        assert path.name == "0001_init.py"
        # __init__.py should be created
        assert (mig_dir / "__init__.py").exists()


# ======================================================================
# Operations (MigrationContext)
# ======================================================================


class TestMigrationContext:
    def test_dry_run_records_ops(self):
        ctx = MigrationContext(dry_run=True)
        ctx.add_unique_constraint("Person", "name")
        ctx.add_index("Person", "age")
        ctx.run_cypher("RETURN 1")
        assert len(ctx.operations) == 3
        assert ctx.operations[0].op_type == "add_unique_constraint"
        assert ctx.operations[1].op_type == "add_index"
        assert ctx.operations[2].op_type == "run_cypher"

    def test_batch_tracking(self):
        ctx = MigrationContext(dry_run=True)
        bid = ctx.begin_batch()
        assert bid is not None
        assert ctx.batch_id == bid
        count = ctx.create_nodes("Person", [{"name": "Alice"}, {"name": "Bob"}])
        assert count == 2
        assert len(ctx.operations) == 2  # begin_batch + create_nodes

    def test_constraint_ops(self):
        ctx = MigrationContext(dry_run=True)
        ctx.add_unique_constraint("Person", "email")
        ctx.drop_unique_constraint("Person", "email")
        ctx.add_node_property_existence_constraint("Person", "name")
        ctx.drop_node_property_existence_constraint("Person", "name")
        assert len(ctx.operations) == 4

    def test_rename_ops(self):
        ctx = MigrationContext(dry_run=True)
        ctx.rename_label("OldPerson", "NewPerson")
        ctx.rename_relationship_type("OLD_REL", "NEW_REL")
        ctx.rename_node_property("Person", "old_name", "new_name")
        assert len(ctx.operations) == 3

    def test_delete_by_batch(self):
        ctx = MigrationContext(dry_run=True)
        ctx.delete_nodes_by_batch("Person", "batch_001")
        ctx.delete_relationships_by_batch("KNOWS", "batch_001")
        assert len(ctx.operations) == 2

    def test_create_relationships(self):
        ctx = MigrationContext(dry_run=True)
        data = [
            {"source_id": "1", "target_id": "2", "properties": {"weight": 1.0}},
        ]
        count = ctx.create_relationships("Person", "Person", "KNOWS", data)
        assert count == 1

    def test_empty_data_returns_zero(self):
        ctx = MigrationContext(dry_run=True)
        assert ctx.create_nodes("Person", []) == 0
        assert ctx.create_relationships("A", "B", "R", []) == 0


# ======================================================================
# Engine
# ======================================================================


def _write_simple_migration(
    mig_dir: Path,
    revision_id: str,
    parent_id: str | None = None,
    rollback_safe: bool = True,
) -> Path:
    """Helper to write a minimal migration file."""
    parent_repr = repr(parent_id)
    content = textwrap.dedent(f"""\
        from crochet.migrations.operations import MigrationContext

        revision_id = "{revision_id}"
        parent_id = {parent_repr}
        schema_hash = "testhash"
        rollback_safe = {rollback_safe}

        def upgrade(ctx: MigrationContext) -> None:
            ctx.run_cypher("RETURN 1")

        def downgrade(ctx: MigrationContext) -> None:
            ctx.run_cypher("RETURN 0")
    """)
    path = mig_dir / f"{revision_id}.py"
    path.write_text(content)
    return path


class TestMigrationEngine:
    def test_discover_empty(self, config, ledger):
        engine = MigrationEngine(config, ledger)
        assert engine.discover_migrations() == []

    def test_discover_ordered(self, config, ledger):
        mig_dir = config.migrations_dir
        _write_simple_migration(mig_dir, "0002_second", parent_id="0001_first")
        _write_simple_migration(mig_dir, "0001_first")
        engine = MigrationEngine(config, ledger)
        migrations = engine.discover_migrations()
        assert [m.revision_id for m in migrations] == [
            "0001_first",
            "0002_second",
        ]

    def test_pending_and_applied(self, config, ledger):
        mig_dir = config.migrations_dir
        _write_simple_migration(mig_dir, "0001_first")
        _write_simple_migration(mig_dir, "0002_second", parent_id="0001_first")
        engine = MigrationEngine(config, ledger)
        assert len(engine.pending_migrations()) == 2
        assert len(engine.applied_migrations()) == 0

    def test_upgrade_all(self, config, ledger):
        mig_dir = config.migrations_dir
        _write_simple_migration(mig_dir, "0001_first")
        _write_simple_migration(mig_dir, "0002_second", parent_id="0001_first")
        engine = MigrationEngine(config, ledger)
        applied = engine.upgrade(dry_run=False)
        assert applied == ["0001_first", "0002_second"]
        assert ledger.is_applied("0001_first")
        assert ledger.is_applied("0002_second")

    def test_upgrade_target(self, config, ledger):
        mig_dir = config.migrations_dir
        _write_simple_migration(mig_dir, "0001_first")
        _write_simple_migration(mig_dir, "0002_second", parent_id="0001_first")
        engine = MigrationEngine(config, ledger)
        applied = engine.upgrade(target="0001_first")
        assert applied == ["0001_first"]

    def test_upgrade_idempotent(self, config, ledger):
        mig_dir = config.migrations_dir
        _write_simple_migration(mig_dir, "0001_first")
        engine = MigrationEngine(config, ledger)
        engine.upgrade()
        applied = engine.upgrade()
        assert applied == []

    def test_downgrade_one(self, config, ledger):
        mig_dir = config.migrations_dir
        _write_simple_migration(mig_dir, "0001_first")
        _write_simple_migration(mig_dir, "0002_second", parent_id="0001_first")
        engine = MigrationEngine(config, ledger)
        engine.upgrade()
        reverted = engine.downgrade()
        assert reverted == ["0002_second"]
        assert ledger.is_applied("0001_first")
        assert not ledger.is_applied("0002_second")

    def test_downgrade_to_target(self, config, ledger):
        mig_dir = config.migrations_dir
        _write_simple_migration(mig_dir, "0001_first")
        _write_simple_migration(mig_dir, "0002_second", parent_id="0001_first")
        _write_simple_migration(mig_dir, "0003_third", parent_id="0002_second")
        engine = MigrationEngine(config, ledger)
        engine.upgrade()
        reverted = engine.downgrade(target="0001_first")
        assert reverted == ["0003_third", "0002_second"]

    def test_downgrade_unsafe_raises(self, config, ledger):
        mig_dir = config.migrations_dir
        _write_simple_migration(mig_dir, "0001_first", rollback_safe=False)
        engine = MigrationEngine(config, ledger)
        engine.upgrade()
        with pytest.raises(RollbackUnsafeError):
            engine.downgrade()

    def test_create_migration(self, config, ledger):
        engine = MigrationEngine(config, ledger)
        path = engine.create_migration("initial setup")
        assert path.exists()
        assert "0001_initial_setup" in path.name
        content = path.read_text()
        assert "parent_id = None" in content

    def test_create_migration_with_snapshot(self, config, ledger, sample_node_file):
        from crochet.ir.parser import parse_models_directory

        snapshot = parse_models_directory(config.models_dir)
        engine = MigrationEngine(config, ledger)
        path = engine.create_migration("add person", current_snapshot=snapshot)
        content = path.read_text()
        assert snapshot.schema_hash[:16] in content

    def test_create_chained_migrations(self, config, ledger):
        engine = MigrationEngine(config, ledger)
        engine.create_migration("first")
        path2 = engine.create_migration("second")
        content = path2.read_text()
        assert "0001_first" in content  # parent_id references first migration

    def test_dry_run_does_not_record(self, config, ledger):
        mig_dir = config.migrations_dir
        _write_simple_migration(mig_dir, "0001_first")
        engine = MigrationEngine(config, ledger)
        applied = engine.upgrade(dry_run=True)
        assert applied == ["0001_first"]
        assert not ledger.is_applied("0001_first")
