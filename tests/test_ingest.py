"""Tests for data ingest and batch tracking."""

from __future__ import annotations

from pathlib import Path

import pytest

from crochet.errors import IngestError
from crochet.ingest.batch import IngestTracker, compute_file_checksum
from crochet.ledger.sqlite import Ledger


class TestFileChecksum:
    def test_checksum(self, tmp_path: Path):
        f = tmp_path / "data.csv"
        f.write_text("a,b,c\n1,2,3\n")
        h1 = compute_file_checksum(f)
        h2 = compute_file_checksum(f)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex

    def test_different_content(self, tmp_path: Path):
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_text("hello")
        f2.write_text("world")
        assert compute_file_checksum(f1) != compute_file_checksum(f2)


class TestIngestTracker:
    def test_register_batch(self, ledger: Ledger):
        tracker = IngestTracker(ledger)
        batch = tracker.register_batch(batch_id="test_batch")
        assert batch.batch_id == "test_batch"
        assert batch.loader_version == "1.0"

    def test_register_batch_with_file(self, ledger: Ledger, tmp_path: Path):
        f = tmp_path / "data.csv"
        f.write_text("a,b\n1,2\n")
        tracker = IngestTracker(ledger)
        batch = tracker.register_batch(
            source_file=f, batch_id="b1", record_count=1
        )
        assert batch.file_checksum is not None
        assert batch.source_file == str(f)
        assert batch.record_count == 1

    def test_register_batch_missing_file(self, ledger: Ledger):
        tracker = IngestTracker(ledger)
        with pytest.raises(IngestError, match="not found"):
            tracker.register_batch(source_file=Path("/nonexistent.csv"))

    def test_verify_file(self, ledger: Ledger, tmp_path: Path):
        f = tmp_path / "data.csv"
        f.write_text("a,b\n1,2\n")
        tracker = IngestTracker(ledger)
        batch = tracker.register_batch(source_file=f, batch_id="b1")
        assert tracker.verify_file(batch) is True

        # Modify the file
        f.write_text("a,b\n1,2\n3,4\n")
        assert tracker.verify_file(batch) is False

    def test_verify_file_deleted(self, ledger: Ledger, tmp_path: Path):
        f = tmp_path / "data.csv"
        f.write_text("hello")
        tracker = IngestTracker(ledger)
        batch = tracker.register_batch(source_file=f, batch_id="b1")
        f.unlink()
        assert tracker.verify_file(batch) is False

    def test_verify_file_no_source(self, ledger: Ledger):
        tracker = IngestTracker(ledger)
        batch = tracker.register_batch(batch_id="b1")
        assert tracker.verify_file(batch) is True  # nothing to verify

    def test_auto_batch_id(self, ledger: Ledger):
        tracker = IngestTracker(ledger)
        batch = tracker.register_batch()
        assert batch.batch_id  # should be auto-generated
        assert len(batch.batch_id) == 12
