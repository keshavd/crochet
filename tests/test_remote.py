"""Tests for remote file fetching, caching, and checksum verification."""

from __future__ import annotations

import hashlib
import http.server
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from crochet.errors import IngestError
from crochet.ingest.batch import IngestTracker, compute_file_checksum
from crochet.ingest.remote import (
    FetcherRegistry,
    FetchResult,
    FileCache,
    GcsFetcher,
    HttpFetcher,
    RemoteSource,
    S3Fetcher,
    fetch_remote,
)
from crochet.ledger.sqlite import Ledger


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    d = tmp_path / "cache"
    d.mkdir()
    return d


@pytest.fixture
def data_file(tmp_path: Path) -> Path:
    f = tmp_path / "sample.tsv"
    f.write_text("id\tname\n1\tAlice\n2\tBob\n")
    return f


@pytest.fixture
def data_checksum(data_file: Path) -> str:
    return compute_file_checksum(data_file)


class _SimpleHandler(http.server.SimpleHTTPRequestHandler):
    """Serves files from a configurable directory."""

    serve_dir: str = "."

    def translate_path(self, path: str) -> str:
        # Override to serve from our temp directory
        import os
        import posixpath
        import urllib.parse

        path = urllib.parse.unquote(path)
        path = posixpath.normpath(path)
        parts = path.split("/")
        parts = [p for p in parts if p]
        result = self.serve_dir
        for part in parts:
            result = os.path.join(result, part)
        return result

    def log_message(self, format, *args):
        pass  # suppress log output during tests


@pytest.fixture
def http_server(tmp_path: Path, data_file: Path):
    """Start a local HTTP server serving the data_file."""
    import shutil

    serve_dir = tmp_path / "serve"
    serve_dir.mkdir()
    shutil.copy(str(data_file), str(serve_dir / "sample.tsv"))

    handler = type(
        "Handler",
        (_SimpleHandler,),
        {"serve_dir": str(serve_dir)},
    )
    server = http.server.HTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


# ---------------------------------------------------------------------------
# RemoteSource
# ---------------------------------------------------------------------------


class TestRemoteSource:
    def test_scheme_http(self):
        src = RemoteSource(uri="https://example.com/data.csv")
        assert src.scheme == "https"

    def test_scheme_s3(self):
        src = RemoteSource(uri="s3://my-bucket/data/file.tsv")
        assert src.scheme == "s3"

    def test_scheme_gs(self):
        src = RemoteSource(uri="gs://my-bucket/data/file.tsv")
        assert src.scheme == "gs"

    def test_default_filename(self):
        src = RemoteSource(uri="https://example.com/path/to/data.csv")
        assert src.default_filename == "data.csv"

    def test_filename_override(self):
        src = RemoteSource(
            uri="https://example.com/path/to/data.csv", filename="override.csv"
        )
        assert src.default_filename == "override.csv"

    def test_default_filename_fallback(self):
        src = RemoteSource(uri="https://example.com/")
        assert src.default_filename == "download"


# ---------------------------------------------------------------------------
# FetcherRegistry
# ---------------------------------------------------------------------------


class TestFetcherRegistry:
    def test_default_registry_has_builtin_schemes(self):
        reg = FetcherRegistry.default()
        assert isinstance(reg.get("http"), HttpFetcher)
        assert isinstance(reg.get("https"), HttpFetcher)
        assert isinstance(reg.get("s3"), S3Fetcher)
        assert isinstance(reg.get("gs"), GcsFetcher)

    def test_unknown_scheme_raises(self):
        reg = FetcherRegistry()
        with pytest.raises(IngestError, match="No fetcher registered"):
            reg.get("ftp")

    def test_case_insensitive(self):
        reg = FetcherRegistry.default()
        assert isinstance(reg.get("HTTP"), HttpFetcher)
        assert isinstance(reg.get("S3"), S3Fetcher)

    def test_custom_fetcher(self):
        from crochet.ingest.remote import Fetcher

        class MyFetcher(Fetcher):
            schemes = ("custom",)

            def fetch(self, source, dest):
                return FetchResult(
                    local_path=dest, uri=source.uri, checksum="abc", size=0
                )

        reg = FetcherRegistry()
        reg.register(MyFetcher())
        assert isinstance(reg.get("custom"), MyFetcher)


# ---------------------------------------------------------------------------
# FileCache
# ---------------------------------------------------------------------------


class TestFileCache:
    def test_store_and_lookup(self, cache_dir: Path, data_file: Path, data_checksum: str):
        cache = FileCache(cache_dir)
        cached_path = cache.store(data_file, data_checksum, "sample.tsv")
        assert cached_path.exists()
        assert cached_path.name == "sample.tsv"

        found = cache.lookup(data_checksum, "sample.tsv")
        assert found is not None
        assert found == cached_path

    def test_lookup_missing(self, cache_dir: Path):
        cache = FileCache(cache_dir)
        assert cache.lookup("deadbeef" * 8, "nope.csv") is None

    def test_lookup_corrupted(self, cache_dir: Path, data_file: Path, data_checksum: str):
        cache = FileCache(cache_dir)
        cached_path = cache.store(data_file, data_checksum, "sample.tsv")

        # Corrupt the cached file
        cached_path.write_text("corrupted content")

        # Lookup should detect the corruption and return None
        result = cache.lookup(data_checksum, "sample.tsv")
        assert result is None
        # The corrupted file should have been removed
        assert not cached_path.exists()

    def test_evict(self, cache_dir: Path, data_file: Path, data_checksum: str):
        cache = FileCache(cache_dir)
        cache.store(data_file, data_checksum, "sample.tsv")

        assert cache.evict(data_checksum) is True
        assert cache.lookup(data_checksum, "sample.tsv") is None
        # Evicting again returns False
        assert cache.evict(data_checksum) is False

    def test_clear(self, cache_dir: Path, data_file: Path, data_checksum: str):
        cache = FileCache(cache_dir)
        cache.store(data_file, data_checksum, "sample.tsv")

        count = cache.clear()
        assert count == 1
        assert cache.lookup(data_checksum, "sample.tsv") is None

    def test_clear_empty(self, cache_dir: Path):
        cache = FileCache(cache_dir)
        assert cache.clear() == 0


# ---------------------------------------------------------------------------
# HttpFetcher
# ---------------------------------------------------------------------------


class TestHttpFetcher:
    def test_fetch_success(self, http_server: str, tmp_path: Path, data_checksum: str):
        fetcher = HttpFetcher()
        source = RemoteSource(uri=f"{http_server}/sample.tsv")
        dest = tmp_path / "out" / "sample.tsv"

        result = fetcher.fetch(source, dest)

        assert result.local_path == dest
        assert result.local_path.exists()
        assert result.checksum == data_checksum
        assert result.size > 0

    def test_fetch_not_found(self, http_server: str, tmp_path: Path):
        fetcher = HttpFetcher()
        source = RemoteSource(uri=f"{http_server}/nonexistent.csv")
        dest = tmp_path / "out" / "nonexistent.csv"

        with pytest.raises(IngestError, match="Failed to fetch"):
            fetcher.fetch(source, dest)

    def test_fetch_bad_host(self, tmp_path: Path):
        fetcher = HttpFetcher(timeout=2)
        source = RemoteSource(uri="http://192.0.2.1/file.csv")  # TEST-NET, unreachable
        dest = tmp_path / "out" / "file.csv"

        with pytest.raises(IngestError, match="Failed to fetch"):
            fetcher.fetch(source, dest)


# ---------------------------------------------------------------------------
# S3Fetcher
# ---------------------------------------------------------------------------


class TestS3Fetcher:
    def test_invalid_uri(self, tmp_path: Path):
        fetcher = S3Fetcher()
        source = RemoteSource(uri="s3:///no-bucket")
        dest = tmp_path / "out.csv"

        with pytest.raises(IngestError, match="Invalid S3 URI"):
            fetcher.fetch(source, dest)

    def test_missing_boto3(self, tmp_path: Path):
        fetcher = S3Fetcher()
        source = RemoteSource(uri="s3://bucket/key.csv")
        dest = tmp_path / "out.csv"

        with patch.dict("sys.modules", {"boto3": None}):
            with pytest.raises(IngestError, match="boto3 is required"):
                fetcher.fetch(source, dest)

    def test_fetch_with_mock_boto3(self, tmp_path: Path, data_file: Path, data_checksum: str):
        """Test S3 fetching using a mocked boto3 client."""
        import shutil

        fetcher = S3Fetcher()
        source = RemoteSource(uri="s3://my-bucket/data/sample.tsv")
        dest = tmp_path / "out" / "sample.tsv"

        mock_client = MagicMock()

        def fake_download(bucket, key, filename):
            shutil.copy(str(data_file), filename)

        mock_client.download_file.side_effect = fake_download

        mock_boto3 = MagicMock()
        mock_boto3.client.return_value = mock_client

        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            result = fetcher.fetch(source, dest)

        assert result.local_path == dest
        assert result.checksum == data_checksum
        mock_client.download_file.assert_called_once()
        call_args = mock_client.download_file.call_args[0]
        assert call_args[0] == "my-bucket"
        assert call_args[1] == "data/sample.tsv"


# ---------------------------------------------------------------------------
# GcsFetcher
# ---------------------------------------------------------------------------


class TestGcsFetcher:
    def test_invalid_uri(self, tmp_path: Path):
        fetcher = GcsFetcher()
        source = RemoteSource(uri="gs:///no-bucket")
        dest = tmp_path / "out.csv"

        with pytest.raises(IngestError, match="Invalid GCS URI"):
            fetcher.fetch(source, dest)

    def test_missing_gcs_library(self, tmp_path: Path):
        fetcher = GcsFetcher()
        source = RemoteSource(uri="gs://bucket/key.csv")
        dest = tmp_path / "out.csv"

        with patch.dict("sys.modules", {"google.cloud": None, "google": None}):
            with pytest.raises(IngestError, match="google-cloud-storage is required"):
                fetcher.fetch(source, dest)

    def test_fetch_with_mock_gcs(self, tmp_path: Path, data_file: Path, data_checksum: str):
        """Test GCS fetching using a mocked google-cloud-storage client."""
        import shutil

        fetcher = GcsFetcher()
        source = RemoteSource(uri="gs://my-bucket/data/sample.tsv")
        dest = tmp_path / "out" / "sample.tsv"

        mock_blob = MagicMock()

        def fake_download(filename):
            shutil.copy(str(data_file), filename)

        mock_blob.download_to_filename.side_effect = fake_download

        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        mock_storage_module = MagicMock()
        mock_storage_module.Client.return_value = mock_client

        mock_google = MagicMock()
        mock_google.cloud.storage = mock_storage_module

        with patch.dict(
            "sys.modules",
            {
                "google": mock_google,
                "google.cloud": mock_google.cloud,
                "google.cloud.storage": mock_storage_module,
            },
        ):
            result = fetcher.fetch(source, dest)

        assert result.local_path == dest
        assert result.checksum == data_checksum
        mock_client.bucket.assert_called_once_with("my-bucket")
        mock_bucket.blob.assert_called_once_with("data/sample.tsv")


# ---------------------------------------------------------------------------
# fetch_remote (integration)
# ---------------------------------------------------------------------------


class TestFetchRemote:
    def test_fetch_and_cache(
        self, http_server: str, cache_dir: Path, data_checksum: str
    ):
        source = RemoteSource(
            uri=f"{http_server}/sample.tsv", expected_checksum=data_checksum
        )
        result = fetch_remote(source, cache_dir=cache_dir)

        assert result.checksum == data_checksum
        assert result.local_path.exists()
        assert result.from_cache is False

        # Fetching again should hit the cache
        result2 = fetch_remote(source, cache_dir=cache_dir)
        assert result2.from_cache is True
        assert result2.checksum == data_checksum

    def test_fetch_checksum_mismatch(self, http_server: str, cache_dir: Path):
        source = RemoteSource(
            uri=f"{http_server}/sample.tsv",
            expected_checksum="0" * 64,
        )
        with pytest.raises(IngestError, match="Checksum mismatch"):
            fetch_remote(source, cache_dir=cache_dir)

    def test_fetch_no_cache(self, http_server: str, tmp_path: Path, data_checksum: str):
        dest_dir = tmp_path / "dest"
        source = RemoteSource(uri=f"{http_server}/sample.tsv")
        result = fetch_remote(source, dest_dir=dest_dir, use_cache=False)

        assert result.checksum == data_checksum
        assert result.local_path.parent == dest_dir

    def test_fetch_string_uri(self, http_server: str, cache_dir: Path, data_checksum: str):
        result = fetch_remote(f"{http_server}/sample.tsv", cache_dir=cache_dir)
        assert result.checksum == data_checksum

    def test_fetch_unsupported_scheme(self, cache_dir: Path):
        with pytest.raises(IngestError, match="No fetcher registered"):
            fetch_remote("ftp://example.com/data.csv", cache_dir=cache_dir)


# ---------------------------------------------------------------------------
# IngestTracker.register_remote_batch
# ---------------------------------------------------------------------------


class TestIngestTrackerRemote:
    def test_register_remote_batch(
        self, ledger: Ledger, http_server: str, tmp_path: Path, data_checksum: str
    ):
        cache_dir = tmp_path / "cache"
        tracker = IngestTracker(ledger)
        batch, local_path = tracker.register_remote_batch(
            f"{http_server}/sample.tsv",
            cache_dir=cache_dir,
            batch_id="remote_batch_1",
        )

        assert batch.batch_id == "remote_batch_1"
        assert batch.file_checksum == data_checksum
        assert batch.source_file == f"{http_server}/sample.tsv"
        assert local_path.exists()

    def test_register_remote_batch_with_checksum(
        self, ledger: Ledger, http_server: str, tmp_path: Path, data_checksum: str
    ):
        cache_dir = tmp_path / "cache"
        tracker = IngestTracker(ledger)
        batch, local_path = tracker.register_remote_batch(
            f"{http_server}/sample.tsv",
            expected_checksum=data_checksum,
            cache_dir=cache_dir,
            batch_id="rb2",
        )
        assert batch.file_checksum == data_checksum

    def test_register_remote_batch_bad_checksum(
        self, ledger: Ledger, http_server: str, tmp_path: Path
    ):
        cache_dir = tmp_path / "cache"
        tracker = IngestTracker(ledger)
        with pytest.raises(IngestError, match="Checksum mismatch"):
            tracker.register_remote_batch(
                f"{http_server}/sample.tsv",
                expected_checksum="0" * 64,
                cache_dir=cache_dir,
                batch_id="rb3",
            )
