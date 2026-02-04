"""Fast file parsing backed by PyArrow with transparent compression support.

Supported formats: CSV, TSV, JSON (record-oriented), JSON Lines, Parquet.
Supported compression: gzip, bzip2, zstd, lz4, xz — detected automatically
from the file extension or explicit ``compression`` parameter.
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Iterator


class FileFormat(Enum):
    CSV = "csv"
    TSV = "tsv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"


# Maps file extensions to compression codecs understood by PyArrow.
_COMPRESSION_MAP: dict[str, str] = {
    ".gz": "gzip",
    ".gzip": "gzip",
    ".bz2": "bz2",
    ".zst": "zstd",
    ".zstd": "zstd",
    ".lz4": "lz4",
    ".xz": "xz",
    ".snappy": "snappy",
}

# Maps bare extensions (after stripping compression suffix) to formats.
_FORMAT_MAP: dict[str, FileFormat] = {
    ".csv": FileFormat.CSV,
    ".tsv": FileFormat.TSV,
    ".tab": FileFormat.TSV,
    ".json": FileFormat.JSON,
    ".jsonl": FileFormat.JSONL,
    ".ndjson": FileFormat.JSONL,
    ".parquet": FileFormat.PARQUET,
    ".pq": FileFormat.PARQUET,
}


def _ensure_pyarrow() -> None:
    try:
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "pyarrow is required for fast file parsing.  Install it with: "
            "pip install 'crochet-migration[data]'"
        ) from exc


def detect_format_and_compression(
    path: Path,
    *,
    fmt: FileFormat | str | None = None,
    compression: str | None = None,
) -> tuple[FileFormat, str | None]:
    """Infer file format and compression from path suffixes.

    Returns ``(format, compression_codec_or_None)``.
    """
    suffixes = [s.lower() for s in path.suffixes]

    # --- Compression ---
    detected_compression = compression
    if detected_compression is None and suffixes:
        last = suffixes[-1]
        if last in _COMPRESSION_MAP:
            detected_compression = _COMPRESSION_MAP[last]
            suffixes = suffixes[:-1]  # strip compression suffix for format detection

    # --- Format ---
    if fmt is not None:
        resolved_fmt = FileFormat(fmt) if isinstance(fmt, str) else fmt
    elif suffixes:
        ext = suffixes[-1]
        resolved_fmt = _FORMAT_MAP.get(ext)
        if resolved_fmt is None:
            supported = ", ".join(sorted(_FORMAT_MAP))
            raise ValueError(
                f"Cannot detect format from extension '{ext}'. "
                f"Supported extensions: {supported}"
            )
    else:
        raise ValueError(
            f"Cannot detect format for '{path.name}'. "
            "Specify the format explicitly."
        )

    return resolved_fmt, detected_compression


@dataclass(frozen=True)
class ParseResult:
    """Result of parsing a data file."""

    records: list[dict[str, Any]]
    row_count: int
    column_names: list[str]
    format: FileFormat
    compression: str | None


def _open_compressed(path: Path, compression: str | None) -> io.BufferedIOBase:
    """Return an open binary file handle, decompressing on the fly if needed.

    Uses Python's standard library decompressors for maximum compatibility.
    """
    if compression is None:
        return open(path, "rb")

    if compression in ("gzip", "gz"):
        import gzip as _gzip
        return _gzip.open(path, "rb")  # type: ignore[return-value]
    elif compression in ("bz2",):
        import bz2 as _bz2
        return _bz2.open(path, "rb")  # type: ignore[return-value]
    elif compression in ("xz", "lzma"):
        import lzma as _lzma
        return _lzma.open(path, "rb")  # type: ignore[return-value]
    elif compression in ("zstd",):
        try:
            import zstandard as _zstd
            fh = open(path, "rb")
            dctx = _zstd.ZstdDecompressor()
            return dctx.stream_reader(fh)  # type: ignore[return-value]
        except ImportError:
            # Fallback to pyarrow's built-in zstd
            import pyarrow as pa
            return pa.CompressedInputStream(pa.OSFile(str(path), "rb"), "zstd")  # type: ignore[return-value]
    elif compression in ("lz4",):
        try:
            import lz4.frame as _lz4
            return _lz4.open(path, "rb")  # type: ignore[return-value]
        except ImportError:
            import pyarrow as pa
            return pa.CompressedInputStream(pa.OSFile(str(path), "rb"), "lz4")  # type: ignore[return-value]
    elif compression in ("snappy",):
        import pyarrow as pa
        return pa.CompressedInputStream(pa.OSFile(str(path), "rb"), "snappy")  # type: ignore[return-value]
    else:
        raise ValueError(f"Unsupported compression: {compression}")


def _parse_csv_tsv(
    path: Path,
    *,
    delimiter: str,
    compression: str | None,
    column_names: list[str] | None,
    skip_rows: int,
    encoding: str,
) -> ParseResult:
    import pyarrow.csv as pcsv

    read_opts = pcsv.ReadOptions(
        column_names=column_names,
        skip_rows=skip_rows,
        encoding=encoding,
    )
    parse_opts = pcsv.ParseOptions(delimiter=delimiter)
    convert_opts = pcsv.ConvertOptions(
        strings_can_be_null=True,
        quoted_strings_can_be_null=True,
    )

    if compression:
        import pyarrow as pa
        py_stream = _open_compressed(path, compression)
        pa_stream = pa.PythonFile(py_stream, mode="r")
        table = pcsv.read_csv(
            pa_stream,
            read_options=read_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        )
    else:
        table = pcsv.read_csv(
            str(path),
            read_options=read_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        )

    records = table.to_pylist()
    fmt = FileFormat.TSV if delimiter == "\t" else FileFormat.CSV
    return ParseResult(
        records=records,
        row_count=len(records),
        column_names=table.column_names,
        format=fmt,
        compression=compression,
    )


def _parse_json(path: Path, *, compression: str | None) -> ParseResult:
    stream = _open_compressed(path, compression)
    raw = stream.read()
    if isinstance(raw, memoryview):
        raw = bytes(raw)
    data = json.loads(raw)

    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        # Try common wrapper keys
        for key in ("data", "records", "results", "rows", "items"):
            if key in data and isinstance(data[key], list):
                records = data[key]
                break
        else:
            records = [data]
    else:
        raise ValueError(f"Expected JSON array or object, got {type(data).__name__}")

    columns = list(records[0].keys()) if records else []
    return ParseResult(
        records=records,
        row_count=len(records),
        column_names=columns,
        format=FileFormat.JSON,
        compression=compression,
    )


def _parse_jsonl(path: Path, *, compression: str | None) -> ParseResult:
    stream = _open_compressed(path, compression)
    raw = stream.read()
    if isinstance(raw, memoryview):
        raw = bytes(raw)
    text = raw.decode("utf-8")
    records = [json.loads(line) for line in text.splitlines() if line.strip()]

    columns = list(records[0].keys()) if records else []
    return ParseResult(
        records=records,
        row_count=len(records),
        column_names=columns,
        format=FileFormat.JSONL,
        compression=compression,
    )


def _parse_parquet(path: Path) -> ParseResult:
    import pyarrow.parquet as pq

    table = pq.read_table(str(path))
    records = table.to_pylist()
    return ParseResult(
        records=records,
        row_count=len(records),
        column_names=table.column_names,
        format=FileFormat.PARQUET,
        compression=None,  # Parquet handles compression internally
    )


def parse_file(
    path: Path | str,
    *,
    fmt: FileFormat | str | None = None,
    compression: str | None = None,
    delimiter: str | None = None,
    column_names: list[str] | None = None,
    skip_rows: int = 0,
    encoding: str = "utf-8",
) -> ParseResult:
    """Parse a data file into a list of dictionaries.

    Parameters
    ----------
    path:
        Local file path.
    fmt:
        Explicit file format.  Detected from extension when ``None``.
    compression:
        Explicit compression codec.  Detected from extension when ``None``.
    delimiter:
        Column delimiter for CSV/TSV.  Defaults to ``,`` for CSV, ``\\t`` for TSV.
    column_names:
        Explicit column names (overrides the header row for CSV/TSV).
    skip_rows:
        Number of rows to skip before reading data (CSV/TSV only).
    encoding:
        Character encoding (CSV/TSV only).  Default ``utf-8``.

    Returns
    -------
    ParseResult
        Contains ``.records`` (list of dicts), ``.row_count``,
        ``.column_names``, ``.format``, and ``.compression``.
    """
    _ensure_pyarrow()

    path = Path(path) if isinstance(path, str) else path
    resolved_fmt, resolved_compression = detect_format_and_compression(
        path, fmt=fmt, compression=compression
    )

    if resolved_fmt in (FileFormat.CSV, FileFormat.TSV):
        if delimiter is None:
            delimiter = "\t" if resolved_fmt == FileFormat.TSV else ","
        return _parse_csv_tsv(
            path,
            delimiter=delimiter,
            compression=resolved_compression,
            column_names=column_names,
            skip_rows=skip_rows,
            encoding=encoding,
        )
    elif resolved_fmt == FileFormat.JSON:
        return _parse_json(path, compression=resolved_compression)
    elif resolved_fmt == FileFormat.JSONL:
        return _parse_jsonl(path, compression=resolved_compression)
    elif resolved_fmt == FileFormat.PARQUET:
        return _parse_parquet(path)
    else:
        raise ValueError(f"Unsupported format: {resolved_fmt}")


def iter_batches(
    path: Path | str,
    *,
    batch_size: int = 10_000,
    fmt: FileFormat | str | None = None,
    compression: str | None = None,
    delimiter: str | None = None,
    column_names: list[str] | None = None,
    skip_rows: int = 0,
    encoding: str = "utf-8",
) -> Iterator[list[dict[str, Any]]]:
    """Yield records in batches for memory-efficient processing of large files.

    Parameters
    ----------
    path:
        Local file path.
    batch_size:
        Number of records per batch.
    fmt, compression, delimiter, column_names, skip_rows, encoding:
        Same as :func:`parse_file`.

    Yields
    ------
    list[dict[str, Any]]
        A batch of records as dictionaries.
    """
    _ensure_pyarrow()

    path = Path(path) if isinstance(path, str) else path
    resolved_fmt, resolved_compression = detect_format_and_compression(
        path, fmt=fmt, compression=compression
    )

    if resolved_fmt in (FileFormat.CSV, FileFormat.TSV):
        if delimiter is None:
            delimiter = "\t" if resolved_fmt == FileFormat.TSV else ","
        yield from _iter_csv_tsv_batches(
            path,
            delimiter=delimiter,
            compression=resolved_compression,
            column_names=column_names,
            skip_rows=skip_rows,
            encoding=encoding,
            batch_size=batch_size,
        )
    elif resolved_fmt == FileFormat.PARQUET:
        yield from _iter_parquet_batches(path, batch_size=batch_size)
    else:
        # For JSON/JSONL, parse all then chunk (these are typically smaller)
        if resolved_fmt == FileFormat.JSON:
            result = _parse_json(path, compression=resolved_compression)
        else:
            result = _parse_jsonl(path, compression=resolved_compression)
        records = result.records
        for i in range(0, len(records), batch_size):
            yield records[i : i + batch_size]


def _iter_csv_tsv_batches(
    path: Path,
    *,
    delimiter: str,
    compression: str | None,
    column_names: list[str] | None,
    skip_rows: int,
    encoding: str,
    batch_size: int,
) -> Iterator[list[dict[str, Any]]]:
    import pyarrow.csv as pcsv

    read_opts = pcsv.ReadOptions(
        column_names=column_names,
        skip_rows=skip_rows,
        encoding=encoding,
        block_size=max(1 << 20, batch_size * 512),  # At least 1 MB blocks
    )
    parse_opts = pcsv.ParseOptions(delimiter=delimiter)
    convert_opts = pcsv.ConvertOptions(
        strings_can_be_null=True,
        quoted_strings_can_be_null=True,
    )

    if compression:
        import pyarrow as pa
        py_stream = _open_compressed(path, compression)
        pa_stream = pa.PythonFile(py_stream, mode="r")
        reader = pcsv.open_csv(
            pa_stream,
            read_options=read_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        )
    else:
        reader = pcsv.open_csv(
            str(path),
            read_options=read_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        )

    buffer: list[dict[str, Any]] = []
    for chunk in reader:
        rows = chunk.to_pylist()
        buffer.extend(rows)
        while len(buffer) >= batch_size:
            yield buffer[:batch_size]
            buffer = buffer[batch_size:]
    if buffer:
        yield buffer


def _iter_parquet_batches(
    path: Path, *, batch_size: int
) -> Iterator[list[dict[str, Any]]]:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(str(path))
    for batch in pf.iter_batches(batch_size=batch_size):
        yield batch.to_pylist()
