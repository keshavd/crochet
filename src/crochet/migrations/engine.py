"""Migration execution engine — ordering, upgrade, downgrade."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from crochet.config import CrochetConfig
from crochet.errors import (
    MigrationChainError,
    MigrationError,
    RollbackUnsafeError,
)
from crochet.ir.diff import SchemaDiff, diff_snapshots
from crochet.ir.hash import hash_snapshot
from crochet.ir.schema import SchemaSnapshot
from crochet.ledger.sqlite import Ledger
from crochet.migrations.operations import MigrationContext
from crochet.migrations.template import (
    generate_revision_id,
    render_migration,
    write_migration_file,
)


class MigrationFile:
    """Represents a loaded migration module."""

    def __init__(self, module: Any, path: Path) -> None:
        self.module = module
        self.path = path
        self.revision_id: str = module.revision_id
        self.parent_id: str | None = module.parent_id
        self.schema_hash: str = module.schema_hash
        self.rollback_safe: bool = getattr(module, "rollback_safe", True)

    def upgrade(self, ctx: MigrationContext) -> None:
        self.module.upgrade(ctx)

    def downgrade(self, ctx: MigrationContext) -> None:
        self.module.downgrade(ctx)


class MigrationEngine:
    """Orchestrates creation, ordering, and execution of migrations."""

    def __init__(self, config: CrochetConfig, ledger: Ledger) -> None:
        self._config = config
        self._ledger = ledger

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover_migrations(self) -> list[MigrationFile]:
        """Load all migration files from the migrations directory, ordered."""
        migrations_dir = self._config.migrations_dir
        if not migrations_dir.exists():
            return []

        files: list[MigrationFile] = []
        for py_file in sorted(migrations_dir.glob("*.py")):
            if py_file.name.startswith("_"):
                continue
            module = self._load_migration_module(py_file)
            if module is None:
                continue
            if not hasattr(module, "revision_id"):
                continue
            files.append(MigrationFile(module, py_file))

        return self._sort_by_chain(files)

    def _load_migration_module(self, path: Path) -> Any:
        mod_name = f"crochet._migrations.{path.stem}"
        spec = importlib.util.spec_from_file_location(mod_name, path)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = module
        spec.loader.exec_module(module)
        return module

    def _sort_by_chain(self, migrations: list[MigrationFile]) -> list[MigrationFile]:
        """Sort migrations by their parent chain (topological order)."""
        by_id = {m.revision_id: m for m in migrations}
        ordered: list[MigrationFile] = []
        visited: set[str] = set()

        # Find root(s)
        roots = [m for m in migrations if m.parent_id is None]
        if not roots and migrations:
            # Fall back to filename sort
            return sorted(migrations, key=lambda m: m.revision_id)

        def walk(m: MigrationFile) -> None:
            if m.revision_id in visited:
                return
            visited.add(m.revision_id)
            if m.parent_id and m.parent_id in by_id:
                walk(by_id[m.parent_id])
            ordered.append(m)

        for root in roots:
            walk(root)

        # Add any orphans not reached by the chain walk
        for m in migrations:
            if m.revision_id not in visited:
                ordered.append(m)

        return ordered

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def pending_migrations(self) -> list[MigrationFile]:
        """Return migrations that have not yet been applied."""
        all_migrations = self.discover_migrations()
        return [m for m in all_migrations if not self._ledger.is_applied(m.revision_id)]

    def applied_migrations(self) -> list[MigrationFile]:
        """Return migrations that have been applied (in order)."""
        all_migrations = self.discover_migrations()
        return [m for m in all_migrations if self._ledger.is_applied(m.revision_id)]

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    def create_migration(
        self,
        description: str,
        current_snapshot: SchemaSnapshot | None = None,
        rollback_safe: bool = True,
    ) -> Path:
        """Scaffold a new migration file.

        If *current_snapshot* is provided, a diff against the previous
        snapshot is computed and included as comments in the migration.
        """
        all_migrations = self.discover_migrations()
        seq = len(all_migrations) + 1
        parent_id: str | None = None
        if all_migrations:
            parent_id = all_migrations[-1].revision_id

        revision_id = generate_revision_id(seq, description)

        # Compute schema hash and diff
        schema_hash = ""
        diff_summary = ""
        if current_snapshot is not None:
            current_snapshot = hash_snapshot(current_snapshot)
            schema_hash = current_snapshot.schema_hash

            # Store the snapshot
            self._ledger.store_snapshot(schema_hash, current_snapshot.to_json())

            # Try to diff against the previous snapshot
            if parent_id and all_migrations:
                prev_hash = all_migrations[-1].schema_hash
                prev_json = self._ledger.get_snapshot(prev_hash)
                if prev_json:
                    prev_snapshot = SchemaSnapshot.from_json(prev_json)
                    diff = diff_snapshots(prev_snapshot, current_snapshot)
                    if diff.has_changes:
                        diff_summary = diff.summary()

        content = render_migration(
            revision_id=revision_id,
            parent_id=parent_id,
            description=description,
            schema_hash=schema_hash,
            rollback_safe=rollback_safe,
            diff_summary=diff_summary,
        )

        return write_migration_file(
            self._config.migrations_dir, revision_id, content
        )

    # ------------------------------------------------------------------
    # Upgrade
    # ------------------------------------------------------------------

    def upgrade(
        self,
        target: str | None = None,
        driver: Any | None = None,
        dry_run: bool = False,
    ) -> list[str]:
        """Apply pending migrations up to *target* (or all).

        Returns the list of applied revision IDs.
        """
        pending = self.pending_migrations()
        if not pending:
            return []

        applied_ids: list[str] = []
        for mf in pending:
            if target and mf.revision_id == target:
                self._apply_one(mf, driver, dry_run)
                applied_ids.append(mf.revision_id)
                break
            self._apply_one(mf, driver, dry_run)
            applied_ids.append(mf.revision_id)
            if target and mf.revision_id == target:
                break

        return applied_ids

    def _apply_one(
        self, mf: MigrationFile, driver: Any | None, dry_run: bool
    ) -> None:
        ctx = MigrationContext(driver=driver, dry_run=dry_run)
        try:
            mf.upgrade(ctx)
        except Exception as exc:
            raise MigrationError(
                f"Migration '{mf.revision_id}' failed during upgrade: {exc}"
            ) from exc

        if not dry_run:
            self._ledger.record_migration(
                revision_id=mf.revision_id,
                parent_id=mf.parent_id,
                description="",
                schema_hash=mf.schema_hash,
                rollback_safe=mf.rollback_safe,
            )

    # ------------------------------------------------------------------
    # Downgrade
    # ------------------------------------------------------------------

    def downgrade(
        self,
        target: str | None = None,
        driver: Any | None = None,
        dry_run: bool = False,
    ) -> list[str]:
        """Revert applied migrations back to *target* (or one step).

        Returns the list of reverted revision IDs.
        """
        applied = list(reversed(self.applied_migrations()))
        if not applied:
            return []

        reverted_ids: list[str] = []
        for mf in applied:
            if target and mf.revision_id == target:
                break
            if not mf.rollback_safe:
                raise RollbackUnsafeError(mf.revision_id)

            ctx = MigrationContext(driver=driver, dry_run=dry_run)
            try:
                mf.downgrade(ctx)
            except Exception as exc:
                raise MigrationError(
                    f"Migration '{mf.revision_id}' failed during downgrade: {exc}"
                ) from exc

            if not dry_run:
                self._ledger.remove_migration(mf.revision_id)

            reverted_ids.append(mf.revision_id)

            # If no target, only revert one step
            if target is None:
                break

        return reverted_ids
