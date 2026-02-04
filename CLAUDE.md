# CLAUDE.md

This file provides context for AI assistants working on the Crochet codebase.

## What is Crochet?

Crochet is a Git-backed, migration-driven framework for versioned schema and data
migrations in Neo4j graphs using neomodel. It is published as `crochet-migration`
on PyPI.

It solves the problem of schema drift in Neo4j by enforcing alignment between
neomodel code, data ingests, and the live graph — without relying on database
introspection.

## Project Structure

```
src/crochet/
├── __init__.py          # Package version (0.1.1)
├── cli.py               # Click-based CLI (main entry point)
├── config.py            # crochet.toml configuration parsing
├── errors.py            # Custom exception hierarchy (15+ types)
├── verify.py            # Project integrity verification
├── ir/                  # Schema Intermediate Representation
│   ├── schema.py        # Data structures: NodeIR, PropertyIR, RelationshipIR, SchemaSnapshot
│   ├── parser.py        # Parses neomodel Python files into IR (no Neo4j needed)
│   ├── hash.py          # Deterministic SHA-256 schema hashing
│   └── diff.py          # Compares two SchemaSnapshots to detect changes
├── migrations/          # Migration execution engine
│   ├── engine.py        # MigrationEngine: discovery, ordering, upgrade/downgrade
│   ├── operations.py    # MigrationContext: 25+ DDL and data operations
│   └── template.py      # Generates migration files from schema diffs
├── ingest/              # Data loading pipeline
│   ├── parsers.py       # PyArrow-backed file parsing (CSV/TSV/JSON/JSONL/Parquet)
│   ├── validate.py      # Declarative data validation framework
│   ├── remote.py        # Remote file fetching (HTTP/S3/GCS) with caching
│   └── batch.py         # Batch tracking and file checksums
├── ledger/              # Migration state tracking
│   └── sqlite.py        # SQLite ledger (applied migrations, batches, snapshots)
└── scaffold/            # Code generation
    ├── node.py          # StructuredNode model scaffolding
    └── relationship.py  # StructuredRel model scaffolding
```

## Build & Test

```bash
# Install for development
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=crochet

# Lint
ruff check src/ tests/

# Type check
mypy src/crochet/
```

## Key Architectural Concepts

- **`__kgid__`**: Every neomodel node/relationship gets an immutable Knowledge
  Graph ID. Models can be renamed or moved across files without losing identity.
- **Schema IR**: neomodel source files are parsed into an intermediate
  representation. No Neo4j connection required for schema comparison or migration
  creation.
- **Hash-chained migrations**: Migrations are parent-chained (Alembic-style).
  Each records a schema hash for drift detection.
- **SQLite ledger**: `.crochet/ledger.db` is the authoritative record of applied
  migrations, dataset batches, and schema snapshots.
- **Batch tracking**: Data loaded via `begin_batch()` is tagged with
  `_crochet_batch` on every node/relationship, enabling delete-by-batch rollback.
- **Rollback safety**: Each migration declares `rollback_safe = True/False`.
  Unsafe migrations refuse to downgrade.

## Dependencies

**Core**: neomodel (>=5.0), click (>=8.0), toml (>=0.10)

**Optional**:
- `pyarrow` (>=14.0) — file parsing (`[data]` extra)
- `boto3` (>=1.28) — S3 remote fetching (`[s3]` extra)
- `google-cloud-storage` (>=2.0) — GCS remote fetching (`[gcs]` extra)

**Dev**: pytest, pytest-cov, ruff, mypy

## Common Patterns

### Adding a new migration operation

Add the method to `MigrationContext` in `src/crochet/migrations/operations.py`.
Follow the existing pattern: construct Cypher, call `self._record_and_run()` with
an operation type string and details dict. All operations are recorded for audit.

### Adding a new CLI command

Add to `src/crochet/cli.py` using Click decorators. Follow the existing pattern of
`@main.command()` with `@click.pass_context`. Use `_get_config(ctx)` and
`_get_ledger(ctx)` for config/ledger access.

### Adding a new remote protocol

Subclass `Fetcher` in `src/crochet/ingest/remote.py`, implement the `fetch()`
method, set the `schemes` class variable, and register it in `_BUILTIN_FETCHERS`.

### Adding a new file format

Add the format to `FileFormat` enum and `_FORMAT_MAP` in
`src/crochet/ingest/parsers.py`, then add a `_parse_<format>()` function and
wire it into `parse_file()`.

## Configuration

Project config lives in `crochet.toml` at the project root. Neo4j credentials
can be overridden with environment variables: `CROCHET_NEO4J_URI`,
`CROCHET_NEO4J_USERNAME`, `CROCHET_NEO4J_PASSWORD`.

## Python Version

Requires Python 3.10+. Uses `from __future__ import annotations` throughout.
