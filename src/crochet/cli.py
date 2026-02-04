"""Crochet CLI — command-line interface for managing neomodel migrations."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from crochet import __version__
from crochet.config import CrochetConfig, load_config, find_project_root
from crochet.errors import CrochetError, ProjectNotInitializedError
from crochet.ledger.sqlite import Ledger


def _get_config(ctx: click.Context) -> CrochetConfig:
    """Load config, attaching it to the Click context."""
    if "config" not in ctx.obj:
        ctx.obj["config"] = load_config()
    return ctx.obj["config"]


def _get_ledger(ctx: click.Context) -> Ledger:
    """Open the ledger, attaching it to the Click context for cleanup."""
    if "ledger" not in ctx.obj:
        config = _get_config(ctx)
        ctx.obj["ledger"] = Ledger(config.ledger_file)
    return ctx.obj["ledger"]


# ======================================================================
# Root group
# ======================================================================


@click.group()
@click.version_option(__version__, prog_name="crochet")
@click.pass_context
def main(ctx: click.Context) -> None:
    """Crochet — versioned schema & data migrations for neomodel graphs."""
    ctx.ensure_object(dict)


# ======================================================================
# new-project
# ======================================================================


@main.command("new-project")
@click.option("--name", default="my-graph", help="Project name.")
@click.option(
    "--path",
    type=click.Path(),
    default=".",
    help="Directory to initialize (default: current directory).",
)
@click.pass_context
def new_project(ctx: click.Context, name: str, path: str) -> None:
    """Initialize a new Crochet project."""
    root = Path(path).resolve()

    config = CrochetConfig(project_name=name, project_root=root)
    config.save()

    # Create directories
    config.models_dir.mkdir(parents=True, exist_ok=True)
    (config.models_dir / "__init__.py").touch()
    config.migrations_dir.mkdir(parents=True, exist_ok=True)
    (config.migrations_dir / "__init__.py").touch()

    # Initialize ledger
    with Ledger(config.ledger_file):
        pass

    click.echo(f"Initialized crochet project '{name}' at {root}")
    click.echo(f"  config:     {root / 'crochet.toml'}")
    click.echo(f"  models:     {config.models_dir}")
    click.echo(f"  migrations: {config.migrations_dir}")
    click.echo(f"  ledger:     {config.ledger_file}")


# ======================================================================
# create-node
# ======================================================================


@main.command("create-node")
@click.argument("class_name")
@click.option("--kgid", default=None, help="Explicit __kgid__ (auto-generated if omitted).")
@click.pass_context
def create_node(ctx: click.Context, class_name: str, kgid: str | None) -> None:
    """Scaffold a new StructuredNode model file."""
    from crochet.scaffold.node import scaffold_node

    config = _get_config(ctx)
    path = scaffold_node(config.models_dir, class_name, kgid=kgid)
    click.echo(f"Created node model: {path}")


# ======================================================================
# create-relationship
# ======================================================================


@main.command("create-relationship")
@click.argument("class_name")
@click.option("--rel-type", default=None, help="Neo4j relationship type (default: CLASS_NAME).")
@click.option("--kgid", default=None, help="Explicit __kgid__ (auto-generated if omitted).")
@click.pass_context
def create_relationship(
    ctx: click.Context, class_name: str, rel_type: str | None, kgid: str | None
) -> None:
    """Scaffold a new StructuredRel model file."""
    from crochet.scaffold.relationship import scaffold_relationship

    config = _get_config(ctx)
    path = scaffold_relationship(
        config.models_dir, class_name, rel_type=rel_type, kgid=kgid
    )
    click.echo(f"Created relationship model: {path}")


# ======================================================================
# create-migration
# ======================================================================


@main.command("create-migration")
@click.argument("description")
@click.option("--no-snapshot", is_flag=True, help="Skip schema snapshot.")
@click.option("--unsafe", is_flag=True, help="Mark migration as rollback-unsafe.")
@click.pass_context
def create_migration(
    ctx: click.Context, description: str, no_snapshot: bool, unsafe: bool
) -> None:
    """Create a new migration file."""
    from crochet.ir.parser import parse_models_directory
    from crochet.migrations.engine import MigrationEngine

    config = _get_config(ctx)
    ledger = _get_ledger(ctx)
    engine = MigrationEngine(config, ledger)

    snapshot = None
    if not no_snapshot:
        try:
            snapshot = parse_models_directory(config.models_dir)
        except CrochetError as exc:
            click.echo(f"Warning: could not parse models: {exc}", err=True)
            snapshot = None

    path = engine.create_migration(
        description=description,
        current_snapshot=snapshot,
        rollback_safe=not unsafe,
    )
    click.echo(f"Created migration: {path}")
    if snapshot:
        click.echo(f"  schema hash: {snapshot.schema_hash[:16]}…")


# ======================================================================
# upgrade
# ======================================================================


@main.command()
@click.option("--target", default=None, help="Migrate up to this revision.")
@click.option("--dry-run", is_flag=True, help="Show what would be applied without executing.")
@click.pass_context
def upgrade(ctx: click.Context, target: str | None, dry_run: bool) -> None:
    """Apply pending migrations."""
    from crochet.migrations.engine import MigrationEngine

    config = _get_config(ctx)
    ledger = _get_ledger(ctx)
    engine = MigrationEngine(config, ledger)

    driver = _try_connect_neo4j(config) if not dry_run else None

    try:
        applied = engine.upgrade(target=target, driver=driver, dry_run=dry_run)
    finally:
        if driver:
            driver.close()

    if not applied:
        click.echo("Nothing to apply — already up to date.")
    else:
        prefix = "[dry-run] " if dry_run else ""
        for rev in applied:
            click.echo(f"{prefix}Applied: {rev}")
        click.echo(f"{prefix}{len(applied)} migration(s) applied.")


# ======================================================================
# downgrade
# ======================================================================


@main.command()
@click.option("--target", default=None, help="Revert down to (but not including) this revision.")
@click.option("--dry-run", is_flag=True, help="Show what would be reverted without executing.")
@click.pass_context
def downgrade(ctx: click.Context, target: str | None, dry_run: bool) -> None:
    """Revert the most recent migration (or down to --target)."""
    from crochet.migrations.engine import MigrationEngine

    config = _get_config(ctx)
    ledger = _get_ledger(ctx)
    engine = MigrationEngine(config, ledger)

    driver = _try_connect_neo4j(config) if not dry_run else None

    try:
        reverted = engine.downgrade(target=target, driver=driver, dry_run=dry_run)
    except CrochetError as exc:
        raise click.ClickException(str(exc)) from exc
    finally:
        if driver:
            driver.close()

    if not reverted:
        click.echo("Nothing to revert.")
    else:
        prefix = "[dry-run] " if dry_run else ""
        for rev in reverted:
            click.echo(f"{prefix}Reverted: {rev}")
        click.echo(f"{prefix}{len(reverted)} migration(s) reverted.")


# ======================================================================
# status
# ======================================================================


@main.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show migration status."""
    from crochet.migrations.engine import MigrationEngine

    config = _get_config(ctx)
    ledger = _get_ledger(ctx)
    engine = MigrationEngine(config, ledger)

    all_migrations = engine.discover_migrations()
    applied = engine.applied_migrations()
    pending = engine.pending_migrations()

    click.echo(f"Project: {config.project_name}")
    click.echo(f"Total migrations:   {len(all_migrations)}")
    click.echo(f"Applied:            {len(applied)}")
    click.echo(f"Pending:            {len(pending)}")

    head = ledger.get_head()
    if head:
        click.echo(f"Head:               {head.revision_id}")
        click.echo(f"  applied at:       {head.applied_at}")
        click.echo(f"  schema hash:      {head.schema_hash[:16]}…" if head.schema_hash else "")
    else:
        click.echo("Head:               (none)")

    if pending:
        click.echo("\nPending migrations:")
        for m in pending:
            safe = "safe" if m.rollback_safe else "UNSAFE"
            click.echo(f"  - {m.revision_id} [{safe}]")

    # Dataset batches
    batches = ledger.get_batches()
    if batches:
        click.echo(f"\nDataset batches:    {len(batches)}")
        for b in batches[-5:]:  # show last 5
            click.echo(f"  - {b.batch_id} ({b.source_file or 'no file'})")


# ======================================================================
# verify
# ======================================================================


@main.command()
@click.option("--with-neo4j", is_flag=True, help="Also verify Neo4j connectivity.")
@click.pass_context
def verify(ctx: click.Context, with_neo4j: bool) -> None:
    """Run verification checks."""
    from crochet.verify import verify_project

    config = _get_config(ctx)
    ledger = _get_ledger(ctx)

    driver = None
    if with_neo4j:
        driver = _try_connect_neo4j(config)

    try:
        report = verify_project(config, ledger, driver=driver)
    finally:
        if driver:
            driver.close()

    click.echo(report.summary())
    if not report.passed:
        raise SystemExit(1)


# ======================================================================
# load-data
# ======================================================================


@main.command("load-data")
@click.argument("path", type=click.Path(exists=True))
@click.option("--format", "fmt", default=None, help="File format (csv, tsv, json, jsonl, parquet).")
@click.option("--validate-only", is_flag=True, help="Only validate, don't show records.")
@click.option("--head", "head_n", type=int, default=5, help="Number of records to preview.")
@click.pass_context
def load_data(
    ctx: click.Context,
    path: str,
    fmt: str | None,
    validate_only: bool,
    head_n: int,
) -> None:
    """Parse and preview a data file (CSV/TSV/JSON/Parquet).

    Use this to inspect a data file before writing a migration that loads it.
    Supports gzip, bzip2, zstd, xz, and lz4 compression transparently.
    """
    try:
        from crochet.ingest.parsers import parse_file
    except ImportError as exc:
        raise click.ClickException(
            "pyarrow is required for file parsing.  "
            "Install it with: pip install 'crochet-migration[data]'"
        ) from exc

    file_path = Path(path)

    try:
        result = parse_file(file_path, fmt=fmt)
    except (ValueError, Exception) as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Format:      {result.format.value}")
    if result.compression:
        click.echo(f"Compression: {result.compression}")
    click.echo(f"Rows:        {result.row_count:,}")
    click.echo(f"Columns:     {', '.join(result.column_names)}")

    if not validate_only and result.records:
        click.echo(f"\nFirst {min(head_n, len(result.records))} records:")
        for i, rec in enumerate(result.records[:head_n]):
            click.echo(f"  [{i}] {rec}")


# ======================================================================
# validate-data
# ======================================================================


@main.command("validate-data")
@click.argument("path", type=click.Path(exists=True))
@click.option("--format", "fmt", default=None, help="File format override.")
@click.option("--require", multiple=True, help="Column that must be non-null (repeatable).")
@click.option("--unique", multiple=True, help="Column that must be unique (repeatable).")
@click.option("--min-rows", type=int, default=None, help="Minimum row count.")
@click.option("--max-rows", type=int, default=None, help="Maximum row count.")
@click.option("--strict", is_flag=True, help="Warn on unexpected columns.")
@click.pass_context
def validate_data_cmd(
    ctx: click.Context,
    path: str,
    fmt: str | None,
    require: tuple[str, ...],
    unique: tuple[str, ...],
    min_rows: int | None,
    max_rows: int | None,
    strict: bool,
) -> None:
    """Validate a data file against column rules.

    Quick validation without writing Python.  For complex schemas, use the
    ``DataSchema`` API in your migration code.
    """
    try:
        from crochet.ingest.parsers import parse_file
        from crochet.ingest.validate import DataSchema, validate
    except ImportError as exc:
        raise click.ClickException(
            "pyarrow is required for file parsing.  "
            "Install it with: pip install 'crochet-migration[data]'"
        ) from exc

    file_path = Path(path)

    try:
        parsed = parse_file(file_path, fmt=fmt)
    except (ValueError, Exception) as exc:
        raise click.ClickException(str(exc)) from exc

    schema = DataSchema(
        strict=strict,
        min_rows=min_rows,
        max_rows=max_rows,
        unique_columns=list(unique),
    )
    for col_name in require:
        schema.column(col_name, required=True)

    result = validate(parsed.records, schema)
    click.echo(result.summary())
    if not result.is_valid:
        raise SystemExit(1)


# ======================================================================
# fetch-data
# ======================================================================


@main.command("fetch-data")
@click.argument("uri")
@click.option("--checksum", default=None, help="Expected SHA-256 checksum of the file.")
@click.option("--filename", default=None, help="Override the local filename.")
@click.option("--dest", type=click.Path(), default=None, help="Destination directory.")
@click.option("--no-cache", is_flag=True, help="Skip the local cache.")
@click.pass_context
def fetch_data(
    ctx: click.Context,
    uri: str,
    checksum: str | None,
    filename: str | None,
    dest: str | None,
    no_cache: bool,
) -> None:
    """Fetch a remote data file (HTTP/S3/GCS) with checksum verification."""
    from crochet.ingest.remote import RemoteSource, fetch_remote

    config = _get_config(ctx)
    cache_dir = config.project_root / ".crochet" / "cache"
    dest_dir = Path(dest) if dest else None

    source = RemoteSource(uri=uri, expected_checksum=checksum, filename=filename)

    try:
        result = fetch_remote(
            source,
            dest_dir=dest_dir,
            cache_dir=cache_dir,
            use_cache=not no_cache,
        )
    except CrochetError as exc:
        raise click.ClickException(str(exc)) from exc

    status = "cached" if result.from_cache else "downloaded"
    click.echo(f"[{status}] {result.uri}")
    click.echo(f"  path:     {result.local_path}")
    click.echo(f"  checksum: {result.checksum}")
    click.echo(f"  size:     {result.size:,} bytes")


# ======================================================================
# cache-clear
# ======================================================================


@main.command("cache-clear")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def cache_clear(ctx: click.Context, yes: bool) -> None:
    """Remove all cached remote data files."""
    from crochet.ingest.remote import FileCache

    config = _get_config(ctx)
    cache_dir = config.project_root / ".crochet" / "cache"

    cache = FileCache(cache_dir)
    if not yes:
        click.confirm(f"Remove all cached files in {cache_dir}?", abort=True)

    count = cache.clear()
    click.echo(f"Removed {count} cached entry(ies).")


# ======================================================================
# cache-verify
# ======================================================================


@main.command("cache-verify")
@click.pass_context
def cache_verify(ctx: click.Context) -> None:
    """Verify integrity of all cached data files."""
    from crochet.ingest.batch import compute_file_checksum
    from crochet.ingest.remote import FileCache

    config = _get_config(ctx)
    cache_dir = config.project_root / ".crochet" / "cache"

    if not cache_dir.exists():
        click.echo("No cache directory found.")
        return

    ok_count = 0
    bad_count = 0

    for entry_dir in sorted(cache_dir.iterdir()):
        if not entry_dir.is_dir() or entry_dir.name.startswith("_"):
            continue
        expected_checksum = entry_dir.name
        for fpath in entry_dir.iterdir():
            if fpath.is_file():
                actual = compute_file_checksum(fpath)
                if actual == expected_checksum:
                    ok_count += 1
                    click.echo(f"  OK  {fpath.name}  ({expected_checksum[:16]}…)")
                else:
                    bad_count += 1
                    click.echo(
                        f"  BAD {fpath.name}  "
                        f"(expected {expected_checksum[:16]}…, "
                        f"got {actual[:16]}…)"
                    )

    click.echo(f"\n{ok_count} OK, {bad_count} corrupted.")
    if bad_count:
        raise SystemExit(1)


# ======================================================================
# Helpers
# ======================================================================


def _try_connect_neo4j(config: CrochetConfig) -> object | None:
    """Try to create a Neo4j driver. Returns None on failure."""
    try:
        from neo4j import GraphDatabase

        return GraphDatabase.driver(
            config.neo4j.uri,
            auth=(config.neo4j.username, config.neo4j.password),
        )
    except Exception:
        return None


if __name__ == "__main__":
    main()
