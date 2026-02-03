"""Deterministic hashing for schema snapshots."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crochet.ir.schema import SchemaSnapshot


def _canonical_json(snapshot: "SchemaSnapshot") -> str:
    """Produce a deterministic JSON string for hashing.

    We exclude ``created_at`` and ``schema_hash`` so that two snapshots with
    identical structure always produce the same hash regardless of when they
    were created.
    """
    d = snapshot.to_dict()
    d.pop("created_at", None)
    d.pop("schema_hash", None)
    return json.dumps(d, sort_keys=True, separators=(",", ":"))


def compute_hash(snapshot: "SchemaSnapshot") -> str:
    """Return the SHA-256 hex digest for a snapshot's canonical form."""
    canonical = _canonical_json(snapshot)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def hash_snapshot(snapshot: "SchemaSnapshot") -> "SchemaSnapshot":
    """Return a new snapshot with ``schema_hash`` populated."""
    h = compute_hash(snapshot)
    return replace(snapshot, schema_hash=h)
