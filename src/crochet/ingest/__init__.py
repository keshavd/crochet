"""Data ingest: parsing, validation, remote fetching, and batch tracking."""

from crochet.ingest.batch import compute_file_checksum, IngestTracker
from crochet.ingest.remote import (
    FetchResult,
    FetcherRegistry,
    FileCache,
    GcsFetcher,
    HttpFetcher,
    RemoteSource,
    S3Fetcher,
    fetch_remote,
)

__all__ = [
    # batch
    "IngestTracker",
    "compute_file_checksum",
    # remote
    "FetchResult",
    "FetcherRegistry",
    "FileCache",
    "GcsFetcher",
    "HttpFetcher",
    "RemoteSource",
    "S3Fetcher",
    "fetch_remote",
]

# Parsers and validation are imported lazily since they depend on optional
# packages (pyarrow).  Users access them via:
#   from crochet.ingest.parsers import parse_file, iter_batches, FileFormat
#   from crochet.ingest.validate import validate, DataSchema, ColumnRule
