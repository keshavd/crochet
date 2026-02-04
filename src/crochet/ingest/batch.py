"""Deterministic data-ingest tracking: checksums, provenance, batch IDs."""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

from crochet.errors import IngestError
from crochet.ledger.sqlite import DatasetBatch, Ledger


def compute_file_checksum(path: Path, algorithm: str = "sha256") -> str:
    """Return the hex digest of a file."""
    h = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


class IngestTracker:
    """High-level helper that ties data loading to the ledger."""

    def __init__(self, ledger: Ledger, loader_version: str = "1.0") -> None:
        self._ledger = ledger
        self._loader_version = loader_version

    def register_batch(
        self,
        source_file: Path | None = None,
        migration_id: str | None = None,
        record_count: int | None = None,
        batch_id: str | None = None,
    ) -> DatasetBatch:
        bid = batch_id or uuid.uuid4().hex[:12]
        checksum = None
        fname = None
        if source_file is not None:
            if not source_file.exists():
                raise IngestError(f"Source file not found: {source_file}")
            checksum = compute_file_checksum(source_file)
            fname = str(source_file)

        return self._ledger.record_batch(
            batch_id=bid,
            migration_id=migration_id,
            source_file=fname,
            file_checksum=checksum,
            loader_version=self._loader_version,
            record_count=record_count,
        )

    def register_remote_batch(
        self,
        uri: str,
        *,
        expected_checksum: str | None = None,
        cache_dir: Path | None = None,
        migration_id: str | None = None,
        record_count: int | None = None,
        batch_id: str | None = None,
    ) -> tuple[DatasetBatch, Path]:
        """Fetch a remote file and register it as a data batch.

        Returns
        -------
        tuple[DatasetBatch, Path]
            The recorded batch and the local path to the downloaded file.
        """
        from crochet.ingest.remote import RemoteSource, fetch_remote

        source = RemoteSource(uri=uri, expected_checksum=expected_checksum)
        result = fetch_remote(source, cache_dir=cache_dir)

        bid = batch_id or uuid.uuid4().hex[:12]
        batch = self._ledger.record_batch(
            batch_id=bid,
            migration_id=migration_id,
            source_file=result.uri,
            file_checksum=result.checksum,
            loader_version=self._loader_version,
            record_count=record_count,
        )
        return batch, result.local_path

    def verify_file(self, batch: DatasetBatch) -> bool:
        """Check that the source file still matches the recorded checksum."""
        if batch.source_file is None or batch.file_checksum is None:
            return True
        path = Path(batch.source_file)
        if not path.exists():
            return False
        return compute_file_checksum(path) == batch.file_checksum
