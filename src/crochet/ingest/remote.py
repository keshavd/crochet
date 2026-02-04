"""Remote file fetching with protocol-based dispatch, local caching, and checksum verification."""

from __future__ import annotations

import hashlib
import shutil
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar
from urllib.parse import urlparse

from crochet.errors import IngestError
from crochet.ingest.batch import compute_file_checksum


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FetchResult:
    """Result of fetching a remote file."""

    local_path: Path
    uri: str
    checksum: str
    size: int
    from_cache: bool = False


@dataclass(frozen=True)
class RemoteSource:
    """Describes a remote data file to fetch.

    Parameters
    ----------
    uri:
        Full URI (e.g. ``https://…``, ``s3://bucket/key``, ``gs://bucket/obj``).
    expected_checksum:
        Optional SHA-256 hex digest.  When set, the downloaded file is
        verified and an ``IngestError`` is raised on mismatch.
    filename:
        Override for the local filename.  Defaults to the basename of the URI
        path component.
    """

    uri: str
    expected_checksum: str | None = None
    filename: str | None = None

    @property
    def scheme(self) -> str:
        return urlparse(self.uri).scheme.lower()

    @property
    def default_filename(self) -> str:
        parsed = urlparse(self.uri)
        basename = Path(parsed.path).name
        return self.filename or basename or "download"


# ---------------------------------------------------------------------------
# Abstract fetcher
# ---------------------------------------------------------------------------


class Fetcher(ABC):
    """Protocol handler that knows how to download files for a URI scheme."""

    #: Schemes this fetcher handles, e.g. ``("https", "http")``.
    schemes: ClassVar[tuple[str, ...]] = ()

    @abstractmethod
    def fetch(self, source: RemoteSource, dest: Path) -> FetchResult:
        """Download *source* to *dest* and return a `FetchResult`.

        Implementations **must** write the file atomically — write to a
        temporary file in the same directory, then rename — so that partial
        downloads are never visible.
        """


# ---------------------------------------------------------------------------
# HTTP / HTTPS fetcher
# ---------------------------------------------------------------------------


class HttpFetcher(Fetcher):
    """Fetch files over HTTP / HTTPS using :mod:`urllib.request`."""

    schemes: ClassVar[tuple[str, ...]] = ("http", "https")

    def __init__(self, timeout: int = 120, chunk_size: int = 8192) -> None:
        self._timeout = timeout
        self._chunk_size = chunk_size

    def fetch(self, source: RemoteSource, dest: Path) -> FetchResult:
        import urllib.request
        import urllib.error

        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_path_str = tempfile.mkstemp(dir=dest.parent, suffix=".part")
        tmp_path = Path(tmp_path_str)

        try:
            req = urllib.request.Request(source.uri, headers={"User-Agent": "crochet"})
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                with open(tmp_fd, "wb") as f:
                    while True:
                        chunk = resp.read(self._chunk_size)
                        if not chunk:
                            break
                        f.write(chunk)

            # Atomic rename
            shutil.move(str(tmp_path), str(dest))
        except urllib.error.URLError as exc:
            tmp_path.unlink(missing_ok=True)
            raise IngestError(f"Failed to fetch {source.uri}: {exc}") from exc
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

        checksum = compute_file_checksum(dest)
        size = dest.stat().st_size
        return FetchResult(
            local_path=dest, uri=source.uri, checksum=checksum, size=size
        )


# ---------------------------------------------------------------------------
# S3 fetcher
# ---------------------------------------------------------------------------


class S3Fetcher(Fetcher):
    """Fetch files from Amazon S3 using :mod:`boto3`."""

    schemes: ClassVar[tuple[str, ...]] = ("s3",)

    def fetch(self, source: RemoteSource, dest: Path) -> FetchResult:
        parsed = urlparse(source.uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise IngestError(
                f"Invalid S3 URI '{source.uri}'. Expected s3://bucket/key"
            )

        try:
            import boto3
        except ImportError as exc:
            raise IngestError(
                "boto3 is required for S3 fetching.  Install it with: "
                "pip install boto3"
            ) from exc

        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_path_str = tempfile.mkstemp(dir=dest.parent, suffix=".part")
        tmp_path = Path(tmp_path_str)

        try:
            import os
            os.close(tmp_fd)
            client = boto3.client("s3")
            client.download_file(bucket, key, str(tmp_path))
            shutil.move(str(tmp_path), str(dest))
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            if "boto" in type(exc).__module__ or "botocore" in type(exc).__module__:
                raise IngestError(f"S3 download failed for {source.uri}: {exc}") from exc
            raise

        checksum = compute_file_checksum(dest)
        size = dest.stat().st_size
        return FetchResult(
            local_path=dest, uri=source.uri, checksum=checksum, size=size
        )


# ---------------------------------------------------------------------------
# GCS fetcher
# ---------------------------------------------------------------------------


class GcsFetcher(Fetcher):
    """Fetch files from Google Cloud Storage using :mod:`google.cloud.storage`."""

    schemes: ClassVar[tuple[str, ...]] = ("gs",)

    def fetch(self, source: RemoteSource, dest: Path) -> FetchResult:
        parsed = urlparse(source.uri)
        bucket_name = parsed.netloc
        blob_name = parsed.path.lstrip("/")
        if not bucket_name or not blob_name:
            raise IngestError(
                f"Invalid GCS URI '{source.uri}'. Expected gs://bucket/object"
            )

        try:
            from google.cloud import storage as gcs
        except ImportError as exc:
            raise IngestError(
                "google-cloud-storage is required for GCS fetching.  Install it "
                "with: pip install google-cloud-storage"
            ) from exc

        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_fd, tmp_path_str = tempfile.mkstemp(dir=dest.parent, suffix=".part")
        tmp_path = Path(tmp_path_str)

        try:
            import os
            os.close(tmp_fd)
            client = gcs.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.download_to_filename(str(tmp_path))
            shutil.move(str(tmp_path), str(dest))
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            mod = type(exc).__module__
            if "google" in mod:
                raise IngestError(f"GCS download failed for {source.uri}: {exc}") from exc
            raise

        checksum = compute_file_checksum(dest)
        size = dest.stat().st_size
        return FetchResult(
            local_path=dest, uri=source.uri, checksum=checksum, size=size
        )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# Built-in fetchers, instantiated lazily.
_BUILTIN_FETCHERS: list[type[Fetcher]] = [HttpFetcher, S3Fetcher, GcsFetcher]


class FetcherRegistry:
    """Maps URI schemes to `Fetcher` instances."""

    def __init__(self) -> None:
        self._fetchers: dict[str, Fetcher] = {}

    def register(self, fetcher: Fetcher) -> None:
        """Register a fetcher for all of its declared schemes."""
        for scheme in fetcher.schemes:
            self._fetchers[scheme.lower()] = fetcher

    def get(self, scheme: str) -> Fetcher:
        """Look up the fetcher for *scheme*, raising `IngestError` if unknown."""
        try:
            return self._fetchers[scheme.lower()]
        except KeyError:
            supported = ", ".join(sorted(self._fetchers)) or "(none)"
            raise IngestError(
                f"No fetcher registered for scheme '{scheme}'. "
                f"Supported schemes: {supported}"
            )

    @classmethod
    def default(cls) -> "FetcherRegistry":
        """Return a registry pre-loaded with the built-in fetchers."""
        reg = cls()
        for fetcher_cls in _BUILTIN_FETCHERS:
            reg.register(fetcher_cls())
        return reg


# ---------------------------------------------------------------------------
# Cache manager
# ---------------------------------------------------------------------------


class FileCache:
    """Content-addressable local cache stored under a project directory.

    Layout::

        <cache_dir>/
            <sha256_hex>/
                <original_filename>

    A file is considered cached if the directory for its checksum exists and
    contains a file whose SHA-256 matches.
    """

    def __init__(self, cache_dir: Path) -> None:
        self._cache_dir = cache_dir
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._cache_dir

    def _slot(self, checksum: str, filename: str) -> Path:
        return self._cache_dir / checksum / filename

    def lookup(self, checksum: str, filename: str) -> Path | None:
        """Return the cached path if the file exists and matches, else ``None``."""
        slot = self._slot(checksum, filename)
        if not slot.exists():
            return None
        if compute_file_checksum(slot) != checksum:
            # Corrupted cache entry — remove it
            slot.unlink(missing_ok=True)
            return None
        return slot

    def store(self, source_path: Path, checksum: str, filename: str) -> Path:
        """Copy *source_path* into the cache.  Returns the cached path."""
        slot = self._slot(checksum, filename)
        slot.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(source_path), str(slot))
        return slot

    def evict(self, checksum: str) -> bool:
        """Remove a cache entry by checksum.  Returns ``True`` if removed."""
        entry_dir = self._cache_dir / checksum
        if entry_dir.exists():
            shutil.rmtree(entry_dir)
            return True
        return False

    def clear(self) -> int:
        """Remove all cached files.  Returns the number of entries removed."""
        count = 0
        for child in self._cache_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
                count += 1
        return count


# ---------------------------------------------------------------------------
# High-level fetch function
# ---------------------------------------------------------------------------

_DEFAULT_CACHE_DIR = Path(".crochet") / "cache"


def fetch_remote(
    source: RemoteSource | str,
    *,
    dest_dir: Path | None = None,
    cache_dir: Path | None = None,
    registry: FetcherRegistry | None = None,
    use_cache: bool = True,
) -> FetchResult:
    """Fetch a remote file, verify its checksum, and return a `FetchResult`.

    Parameters
    ----------
    source:
        A `RemoteSource` or a plain URI string.
    dest_dir:
        Directory to place the downloaded file.  Defaults to a temporary
        directory (the cache is the canonical storage location).
    cache_dir:
        Root of the local cache.  Defaults to ``.crochet/cache``.
    registry:
        A `FetcherRegistry`.  The default registry is used when ``None``.
    use_cache:
        When ``True`` (default), check the cache before downloading and
        store the file in the cache after downloading.

    Returns
    -------
    FetchResult
        Includes the local path, URI, SHA-256 checksum, and file size.

    Raises
    ------
    IngestError
        On download failure or checksum mismatch.
    """
    if isinstance(source, str):
        source = RemoteSource(uri=source)

    reg = registry or FetcherRegistry.default()
    cache = FileCache(cache_dir or _DEFAULT_CACHE_DIR) if use_cache else None
    filename = source.default_filename

    # --- Check cache ---
    if cache and source.expected_checksum:
        cached = cache.lookup(source.expected_checksum, filename)
        if cached is not None:
            return FetchResult(
                local_path=cached,
                uri=source.uri,
                checksum=source.expected_checksum,
                size=cached.stat().st_size,
                from_cache=True,
            )

    # --- Determine destination ---
    target_dir = dest_dir or (cache.root / "_staging" if cache else Path(tempfile.mkdtemp()))
    target_dir.mkdir(parents=True, exist_ok=True)
    dest = target_dir / filename

    # --- Fetch ---
    fetcher = reg.get(source.scheme)
    result = fetcher.fetch(source, dest)

    # --- Verify checksum ---
    if source.expected_checksum and result.checksum != source.expected_checksum:
        dest.unlink(missing_ok=True)
        raise IngestError(
            f"Checksum mismatch for {source.uri}: "
            f"expected {source.expected_checksum}, got {result.checksum}"
        )

    # --- Store in cache ---
    if cache:
        cached_path = cache.store(dest, result.checksum, filename)
        # Clean up staging copy if it's different from the cache path
        if dest != cached_path:
            dest.unlink(missing_ok=True)
        result = FetchResult(
            local_path=cached_path,
            uri=result.uri,
            checksum=result.checksum,
            size=result.size,
            from_cache=False,
        )

    return result
