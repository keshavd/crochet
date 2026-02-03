"""Migration file scaffolding and template generation."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

_MIGRATION_TEMPLATE = '''\
"""
{description}

Revision: {revision_id}
Parent:   {parent_id}
Created:  {created_at}
Schema:   {schema_hash}
"""

from crochet.migrations.operations import MigrationContext

# -- Migration metadata --------------------------------------------------

revision_id = "{revision_id}"
parent_id = {parent_id_repr}
schema_hash = "{schema_hash}"
rollback_safe = {rollback_safe}


def upgrade(ctx: MigrationContext) -> None:
    """Apply this migration."""
{upgrade_body}


def downgrade(ctx: MigrationContext) -> None:
    """Revert this migration."""
{downgrade_body}
'''

_DIFF_COMMENT_HEADER = "    # Detected schema changes:\n"


def slugify(text: str) -> str:
    """Convert a description into a filesystem-safe slug."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")[:60]


def generate_revision_id(seq: int, description: str) -> str:
    """Generate a revision id like ``0001_initial``."""
    slug = slugify(description)
    return f"{seq:04d}_{slug}"


def render_migration(
    revision_id: str,
    parent_id: str | None,
    description: str,
    schema_hash: str,
    rollback_safe: bool = True,
    diff_summary: str = "",
) -> str:
    """Render a migration file from template."""
    now = datetime.now(timezone.utc).isoformat()

    if diff_summary:
        upgrade_lines = _DIFF_COMMENT_HEADER
        for line in diff_summary.splitlines():
            upgrade_lines += f"    # {line}\n"
        upgrade_lines += "    pass"
        downgrade_lines = upgrade_lines
    else:
        upgrade_lines = "    pass"
        downgrade_lines = "    pass"

    return _MIGRATION_TEMPLATE.format(
        description=description,
        revision_id=revision_id,
        parent_id=parent_id or "None",
        parent_id_repr=repr(parent_id),
        created_at=now,
        schema_hash=schema_hash,
        rollback_safe=rollback_safe,
        upgrade_body=upgrade_lines,
        downgrade_body=downgrade_lines,
    )


def write_migration_file(
    migrations_dir: Path,
    revision_id: str,
    content: str,
) -> Path:
    """Write a migration file to disk and return the path."""
    migrations_dir.mkdir(parents=True, exist_ok=True)

    # Ensure __init__.py exists
    init_path = migrations_dir / "__init__.py"
    if not init_path.exists():
        init_path.write_text("")

    filename = f"{revision_id}.py"
    file_path = migrations_dir / filename
    file_path.write_text(content)
    return file_path
