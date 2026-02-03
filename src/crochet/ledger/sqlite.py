"""SQLite ledger — the authoritative record of applied migrations and data batches."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from crochet.errors import LedgerError, LedgerIntegrityError

_SCHEMA_VERSION = 1

_INIT_SQL = """\
CREATE TABLE IF NOT EXISTS ledger_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS applied_migrations (
    revision_id   TEXT PRIMARY KEY,
    parent_id     TEXT,
    description   TEXT NOT NULL DEFAULT '',
    schema_hash   TEXT NOT NULL,
    applied_at    TEXT NOT NULL,
    rollback_safe INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS dataset_batches (
    batch_id       TEXT PRIMARY KEY,
    migration_id   TEXT,
    source_file    TEXT,
    file_checksum  TEXT,
    loader_version TEXT,
    record_count   INTEGER,
    created_at     TEXT NOT NULL,
    FOREIGN KEY (migration_id) REFERENCES applied_migrations(revision_id)
);

CREATE TABLE IF NOT EXISTS schema_snapshots (
    schema_hash   TEXT PRIMARY KEY,
    snapshot_json TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
"""


@dataclass
class AppliedMigration:
    revision_id: str
    parent_id: str | None
    description: str
    schema_hash: str
    applied_at: str
    rollback_safe: bool


@dataclass
class DatasetBatch:
    batch_id: str
    migration_id: str | None
    source_file: str | None
    file_checksum: str | None
    loader_version: str | None
    record_count: int | None
    created_at: str


class Ledger:
    """SQLite-backed ledger for migration and data-batch tracking."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _init_schema(self) -> None:
        self._conn.executescript(_INIT_SQL)
        cur = self._conn.execute(
            "SELECT value FROM ledger_meta WHERE key = 'schema_version'"
        )
        row = cur.fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO ledger_meta (key, value) VALUES (?, ?)",
                ("schema_version", str(_SCHEMA_VERSION)),
            )
            self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Ledger":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Migrations
    # ------------------------------------------------------------------

    def record_migration(
        self,
        revision_id: str,
        parent_id: str | None,
        description: str,
        schema_hash: str,
        rollback_safe: bool = True,
    ) -> AppliedMigration:
        now = datetime.now(timezone.utc).isoformat()
        try:
            self._conn.execute(
                """INSERT INTO applied_migrations
                   (revision_id, parent_id, description, schema_hash, applied_at, rollback_safe)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (revision_id, parent_id, description, schema_hash, now, int(rollback_safe)),
            )
            self._conn.commit()
        except sqlite3.IntegrityError as exc:
            raise LedgerError(
                f"Migration '{revision_id}' is already recorded in the ledger."
            ) from exc
        return AppliedMigration(
            revision_id=revision_id,
            parent_id=parent_id,
            description=description,
            schema_hash=schema_hash,
            applied_at=now,
            rollback_safe=rollback_safe,
        )

    def remove_migration(self, revision_id: str) -> None:
        self._conn.execute(
            "DELETE FROM applied_migrations WHERE revision_id = ?", (revision_id,)
        )
        self._conn.commit()

    def get_applied_migrations(self) -> list[AppliedMigration]:
        cur = self._conn.execute(
            "SELECT revision_id, parent_id, description, schema_hash, applied_at, rollback_safe "
            "FROM applied_migrations ORDER BY applied_at"
        )
        return [
            AppliedMigration(
                revision_id=row[0],
                parent_id=row[1],
                description=row[2],
                schema_hash=row[3],
                applied_at=row[4],
                rollback_safe=bool(row[5]),
            )
            for row in cur.fetchall()
        ]

    def get_head(self) -> AppliedMigration | None:
        migrations = self.get_applied_migrations()
        return migrations[-1] if migrations else None

    def is_applied(self, revision_id: str) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM applied_migrations WHERE revision_id = ?", (revision_id,)
        )
        return cur.fetchone() is not None

    # ------------------------------------------------------------------
    # Dataset batches
    # ------------------------------------------------------------------

    def record_batch(
        self,
        batch_id: str,
        migration_id: str | None = None,
        source_file: str | None = None,
        file_checksum: str | None = None,
        loader_version: str | None = None,
        record_count: int | None = None,
    ) -> DatasetBatch:
        now = datetime.now(timezone.utc).isoformat()
        try:
            self._conn.execute(
                """INSERT INTO dataset_batches
                   (batch_id, migration_id, source_file, file_checksum,
                    loader_version, record_count, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (batch_id, migration_id, source_file, file_checksum,
                 loader_version, record_count, now),
            )
            self._conn.commit()
        except sqlite3.IntegrityError as exc:
            raise LedgerError(
                f"Batch '{batch_id}' is already recorded in the ledger."
            ) from exc
        return DatasetBatch(
            batch_id=batch_id,
            migration_id=migration_id,
            source_file=source_file,
            file_checksum=file_checksum,
            loader_version=loader_version,
            record_count=record_count,
            created_at=now,
        )

    def get_batches(self, migration_id: str | None = None) -> list[DatasetBatch]:
        if migration_id:
            cur = self._conn.execute(
                "SELECT batch_id, migration_id, source_file, file_checksum, "
                "loader_version, record_count, created_at "
                "FROM dataset_batches WHERE migration_id = ? ORDER BY created_at",
                (migration_id,),
            )
        else:
            cur = self._conn.execute(
                "SELECT batch_id, migration_id, source_file, file_checksum, "
                "loader_version, record_count, created_at "
                "FROM dataset_batches ORDER BY created_at"
            )
        return [
            DatasetBatch(
                batch_id=row[0],
                migration_id=row[1],
                source_file=row[2],
                file_checksum=row[3],
                loader_version=row[4],
                record_count=row[5],
                created_at=row[6],
            )
            for row in cur.fetchall()
        ]

    def remove_batch(self, batch_id: str) -> None:
        self._conn.execute(
            "DELETE FROM dataset_batches WHERE batch_id = ?", (batch_id,)
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Schema snapshots
    # ------------------------------------------------------------------

    def store_snapshot(self, schema_hash: str, snapshot_json: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """INSERT OR REPLACE INTO schema_snapshots
               (schema_hash, snapshot_json, created_at) VALUES (?, ?, ?)""",
            (schema_hash, snapshot_json, now),
        )
        self._conn.commit()

    def get_snapshot(self, schema_hash: str) -> str | None:
        cur = self._conn.execute(
            "SELECT snapshot_json FROM schema_snapshots WHERE schema_hash = ?",
            (schema_hash,),
        )
        row = cur.fetchone()
        return row[0] if row else None

    # ------------------------------------------------------------------
    # Integrity
    # ------------------------------------------------------------------

    def verify_chain(self) -> list[str]:
        """Verify the parent-chain integrity. Returns a list of issues."""
        issues: list[str] = []
        migrations = self.get_applied_migrations()
        ids = {m.revision_id for m in migrations}

        for m in migrations:
            if m.parent_id is not None and m.parent_id not in ids:
                issues.append(
                    f"Migration '{m.revision_id}' references unknown parent "
                    f"'{m.parent_id}'."
                )
        return issues
