"""Tests for the fast file parser module (pyarrow-backed)."""

from __future__ import annotations

import gzip
import bz2
import json
from pathlib import Path

import pytest

from crochet.ingest.parsers import (
    FileFormat,
    ParseResult,
    detect_format_and_compression,
    iter_batches,
    parse_file,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CSV_CONTENT = "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n"
TSV_CONTENT = "id\tname\tage\n1\tAlice\t30\n2\tBob\t25\n3\tCharlie\t35\n"
JSON_CONTENT = json.dumps([
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 35},
])
JSONL_CONTENT = "\n".join([
    '{"id": 1, "name": "Alice", "age": 30}',
    '{"id": 2, "name": "Bob", "age": 25}',
    '{"id": 3, "name": "Charlie", "age": 35}',
])
JSON_WRAPPED = json.dumps({
    "data": [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]
})


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    f = tmp_path / "data.csv"
    f.write_text(CSV_CONTENT)
    return f


@pytest.fixture
def tsv_file(tmp_path: Path) -> Path:
    f = tmp_path / "data.tsv"
    f.write_text(TSV_CONTENT)
    return f


@pytest.fixture
def json_file(tmp_path: Path) -> Path:
    f = tmp_path / "data.json"
    f.write_text(JSON_CONTENT)
    return f


@pytest.fixture
def jsonl_file(tmp_path: Path) -> Path:
    f = tmp_path / "data.jsonl"
    f.write_text(JSONL_CONTENT)
    return f


@pytest.fixture
def parquet_file(tmp_path: Path) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
    })
    f = tmp_path / "data.parquet"
    pq.write_table(table, str(f))
    return f


@pytest.fixture
def csv_gz_file(tmp_path: Path) -> Path:
    f = tmp_path / "data.csv.gz"
    with gzip.open(str(f), "wt") as gz:
        gz.write(CSV_CONTENT)
    return f


@pytest.fixture
def tsv_bz2_file(tmp_path: Path) -> Path:
    f = tmp_path / "data.tsv.bz2"
    with bz2.open(str(f), "wt") as bz:
        bz.write(TSV_CONTENT)
    return f


@pytest.fixture
def json_gz_file(tmp_path: Path) -> Path:
    f = tmp_path / "data.json.gz"
    with gzip.open(str(f), "wb") as gz:
        gz.write(JSON_CONTENT.encode())
    return f


@pytest.fixture
def jsonl_gz_file(tmp_path: Path) -> Path:
    f = tmp_path / "data.jsonl.gz"
    with gzip.open(str(f), "wb") as gz:
        gz.write(JSONL_CONTENT.encode())
    return f


@pytest.fixture
def json_wrapped_file(tmp_path: Path) -> Path:
    f = tmp_path / "wrapped.json"
    f.write_text(JSON_WRAPPED)
    return f


@pytest.fixture
def large_csv_file(tmp_path: Path) -> Path:
    """Create a CSV with 100 rows for batching tests."""
    lines = ["id,name,value"]
    for i in range(100):
        lines.append(f"{i},item_{i},{i * 10}")
    f = tmp_path / "large.csv"
    f.write_text("\n".join(lines) + "\n")
    return f


# ---------------------------------------------------------------------------
# detect_format_and_compression
# ---------------------------------------------------------------------------


class TestDetectFormatAndCompression:
    def test_csv(self):
        fmt, comp = detect_format_and_compression(Path("data.csv"))
        assert fmt == FileFormat.CSV
        assert comp is None

    def test_tsv(self):
        fmt, comp = detect_format_and_compression(Path("data.tsv"))
        assert fmt == FileFormat.TSV
        assert comp is None

    def test_json(self):
        fmt, comp = detect_format_and_compression(Path("data.json"))
        assert fmt == FileFormat.JSON
        assert comp is None

    def test_jsonl(self):
        fmt, comp = detect_format_and_compression(Path("data.jsonl"))
        assert fmt == FileFormat.JSONL
        assert comp is None

    def test_ndjson(self):
        fmt, comp = detect_format_and_compression(Path("data.ndjson"))
        assert fmt == FileFormat.JSONL
        assert comp is None

    def test_parquet(self):
        fmt, comp = detect_format_and_compression(Path("data.parquet"))
        assert fmt == FileFormat.PARQUET
        assert comp is None

    def test_pq(self):
        fmt, comp = detect_format_and_compression(Path("data.pq"))
        assert fmt == FileFormat.PARQUET
        assert comp is None

    def test_csv_gz(self):
        fmt, comp = detect_format_and_compression(Path("data.csv.gz"))
        assert fmt == FileFormat.CSV
        assert comp == "gzip"

    def test_tsv_bz2(self):
        fmt, comp = detect_format_and_compression(Path("data.tsv.bz2"))
        assert fmt == FileFormat.TSV
        assert comp == "bz2"

    def test_json_zst(self):
        fmt, comp = detect_format_and_compression(Path("data.json.zst"))
        assert fmt == FileFormat.JSON
        assert comp == "zstd"

    def test_csv_xz(self):
        fmt, comp = detect_format_and_compression(Path("data.csv.xz"))
        assert fmt == FileFormat.CSV
        assert comp == "xz"

    def test_explicit_format(self):
        fmt, comp = detect_format_and_compression(
            Path("mystery_file"), fmt=FileFormat.CSV
        )
        assert fmt == FileFormat.CSV

    def test_explicit_format_string(self):
        fmt, comp = detect_format_and_compression(
            Path("mystery_file"), fmt="tsv"
        )
        assert fmt == FileFormat.TSV

    def test_explicit_compression(self):
        fmt, comp = detect_format_and_compression(
            Path("data.csv"), compression="gzip"
        )
        assert comp == "gzip"

    def test_unknown_extension(self):
        with pytest.raises(ValueError, match="Cannot detect format"):
            detect_format_and_compression(Path("data.xyz"))

    def test_no_extension(self):
        with pytest.raises(ValueError, match="Cannot detect format"):
            detect_format_and_compression(Path("noext"))


# ---------------------------------------------------------------------------
# parse_file — CSV
# ---------------------------------------------------------------------------


class TestParseCSV:
    def test_basic(self, csv_file: Path):
        result = parse_file(csv_file)
        assert result.format == FileFormat.CSV
        assert result.row_count == 3
        assert result.column_names == ["id", "name", "age"]
        assert result.compression is None
        assert result.records[0]["name"] == "Alice"

    def test_compressed_gzip(self, csv_gz_file: Path):
        result = parse_file(csv_gz_file)
        assert result.format == FileFormat.CSV
        assert result.compression == "gzip"
        assert result.row_count == 3
        assert result.records[1]["name"] == "Bob"

    def test_string_path(self, csv_file: Path):
        result = parse_file(str(csv_file))
        assert result.row_count == 3


# ---------------------------------------------------------------------------
# parse_file — TSV
# ---------------------------------------------------------------------------


class TestParseTSV:
    def test_basic(self, tsv_file: Path):
        result = parse_file(tsv_file)
        assert result.format == FileFormat.TSV
        assert result.row_count == 3
        assert result.records[2]["name"] == "Charlie"

    def test_compressed_bz2(self, tsv_bz2_file: Path):
        result = parse_file(tsv_bz2_file)
        assert result.format == FileFormat.TSV
        assert result.compression == "bz2"
        assert result.row_count == 3

    def test_explicit_delimiter(self, tsv_file: Path):
        result = parse_file(tsv_file, delimiter="\t")
        assert result.row_count == 3


# ---------------------------------------------------------------------------
# parse_file — JSON
# ---------------------------------------------------------------------------


class TestParseJSON:
    def test_array(self, json_file: Path):
        result = parse_file(json_file)
        assert result.format == FileFormat.JSON
        assert result.row_count == 3
        assert result.records[0]["name"] == "Alice"

    def test_wrapped_object(self, json_wrapped_file: Path):
        result = parse_file(json_wrapped_file)
        assert result.row_count == 2
        assert result.records[0]["name"] == "Alice"

    def test_compressed_gzip(self, json_gz_file: Path):
        result = parse_file(json_gz_file)
        assert result.compression == "gzip"
        assert result.row_count == 3

    def test_single_object(self, tmp_path: Path):
        f = tmp_path / "single.json"
        f.write_text('{"id": 1, "name": "Alice"}')
        result = parse_file(f)
        assert result.row_count == 1
        assert result.records[0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# parse_file — JSONL
# ---------------------------------------------------------------------------


class TestParseJSONL:
    def test_basic(self, jsonl_file: Path):
        result = parse_file(jsonl_file)
        assert result.format == FileFormat.JSONL
        assert result.row_count == 3

    def test_compressed_gzip(self, jsonl_gz_file: Path):
        result = parse_file(jsonl_gz_file)
        assert result.compression == "gzip"
        assert result.row_count == 3


# ---------------------------------------------------------------------------
# parse_file — Parquet
# ---------------------------------------------------------------------------


class TestParseParquet:
    def test_basic(self, parquet_file: Path):
        result = parse_file(parquet_file)
        assert result.format == FileFormat.PARQUET
        assert result.row_count == 3
        assert set(result.column_names) == {"id", "name", "age"}
        assert result.records[0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# iter_batches
# ---------------------------------------------------------------------------


class TestIterBatches:
    def test_csv_batches(self, large_csv_file: Path):
        batches = list(iter_batches(large_csv_file, batch_size=30))
        assert len(batches) == 4  # 100 rows / 30 = 3 full + 1 partial
        assert len(batches[0]) == 30
        assert len(batches[-1]) == 10
        # Total records
        total = sum(len(b) for b in batches)
        assert total == 100

    def test_json_batches(self, json_file: Path):
        batches = list(iter_batches(json_file, batch_size=2))
        assert len(batches) == 2  # 3 rows / 2 = 1 full + 1 partial
        assert len(batches[0]) == 2
        assert len(batches[1]) == 1

    def test_jsonl_batches(self, jsonl_file: Path):
        batches = list(iter_batches(jsonl_file, batch_size=2))
        assert len(batches) == 2

    def test_parquet_batches(self, parquet_file: Path):
        batches = list(iter_batches(parquet_file, batch_size=2))
        total = sum(len(b) for b in batches)
        assert total == 3

    def test_single_batch(self, csv_file: Path):
        batches = list(iter_batches(csv_file, batch_size=1000))
        assert len(batches) == 1
        assert len(batches[0]) == 3

    def test_compressed_csv_batches(self, csv_gz_file: Path):
        batches = list(iter_batches(csv_gz_file, batch_size=2))
        total = sum(len(b) for b in batches)
        assert total == 3
