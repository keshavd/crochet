"""Migration file scaffolding and template generation."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crochet.ir.diff import SchemaDiff

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


def generate_operations_from_diff(diff: "SchemaDiff") -> tuple[str, str]:
    """Generate upgrade and downgrade operation code from a SchemaDiff.
    
    Returns a tuple of (upgrade_code, downgrade_code) as strings.
    """
    upgrade_lines: list[str] = []
    downgrade_lines: list[str] = []
    
    # Process node changes
    for nc in diff.node_changes:
        if nc.kind == "added":
            # Node added - no automatic operations (user should handle data)
            upgrade_lines.append(f"# TODO: Handle new node '{nc.new.label}' (kgid={nc.kgid})")
            downgrade_lines.append(f"# TODO: Clean up node '{nc.new.label}' (kgid={nc.kgid})")
        elif nc.kind == "removed":
            # Node removed - no automatic operations (user should handle data)
            upgrade_lines.append(f"# TODO: Handle removed node '{nc.old.label}' (kgid={nc.kgid})")
            downgrade_lines.append(f"# TODO: Restore node '{nc.old.label}' (kgid={nc.kgid})")
        elif nc.kind == "modified":
            label = nc.new.label if nc.new else nc.old.label
            
            # Handle label rename
            if nc.label_renamed and nc.old and nc.new:
                upgrade_lines.append(
                    f'ctx.rename_label("{nc.old.label}", "{nc.new.label}")'
                )
                downgrade_lines.append(
                    f'ctx.rename_label("{nc.new.label}", "{nc.old.label}")'
                )
            
            # Handle property changes
            for pc in nc.property_changes:
                if pc.kind == "added":
                    # Property added
                    upgrade_lines.append(
                        f'ctx.add_node_property("{label}", "{pc.property_name}")'
                    )
                    downgrade_lines.append(
                        f'ctx.remove_node_property("{label}", "{pc.property_name}")'
                    )
                    
                    # Handle constraints/indexes for new property
                    if pc.new:
                        if pc.new.unique_index:
                            upgrade_lines.append(
                                f'ctx.add_unique_constraint("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.drop_unique_constraint("{label}", "{pc.property_name}")'
                            )
                        elif pc.new.index:
                            upgrade_lines.append(
                                f'ctx.add_index("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.drop_index("{label}", "{pc.property_name}")'
                            )
                        if pc.new.required:
                            upgrade_lines.append(
                                f'ctx.add_node_property_existence_constraint("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.drop_node_property_existence_constraint("{label}", "{pc.property_name}")'
                            )
                            
                elif pc.kind == "removed":
                    # Property removed
                    upgrade_lines.append(
                        f'ctx.remove_node_property("{label}", "{pc.property_name}")'
                    )
                    downgrade_lines.append(
                        f'ctx.add_node_property("{label}", "{pc.property_name}")'
                    )
                    
                    # Handle constraints/indexes for removed property
                    if pc.old:
                        if pc.old.unique_index:
                            upgrade_lines.append(
                                f'ctx.drop_unique_constraint("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.add_unique_constraint("{label}", "{pc.property_name}")'
                            )
                        elif pc.old.index:
                            upgrade_lines.append(
                                f'ctx.drop_index("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.add_index("{label}", "{pc.property_name}")'
                            )
                        if pc.old.required:
                            upgrade_lines.append(
                                f'ctx.drop_node_property_existence_constraint("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.add_node_property_existence_constraint("{label}", "{pc.property_name}")'
                            )
                            
                elif pc.kind == "modified" and pc.old and pc.new:
                    # Property modified - handle constraint/index changes
                    if pc.old.unique_index != pc.new.unique_index:
                        if pc.new.unique_index:
                            upgrade_lines.append(
                                f'ctx.add_unique_constraint("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.drop_unique_constraint("{label}", "{pc.property_name}")'
                            )
                        else:
                            upgrade_lines.append(
                                f'ctx.drop_unique_constraint("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.add_unique_constraint("{label}", "{pc.property_name}")'
                            )
                    
                    if pc.old.index != pc.new.index:
                        if pc.new.index:
                            upgrade_lines.append(
                                f'ctx.add_index("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.drop_index("{label}", "{pc.property_name}")'
                            )
                        else:
                            upgrade_lines.append(
                                f'ctx.drop_index("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.add_index("{label}", "{pc.property_name}")'
                            )
                    
                    if pc.old.required != pc.new.required:
                        if pc.new.required:
                            upgrade_lines.append(
                                f'ctx.add_node_property_existence_constraint("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.drop_node_property_existence_constraint("{label}", "{pc.property_name}")'
                            )
                        else:
                            upgrade_lines.append(
                                f'ctx.drop_node_property_existence_constraint("{label}", "{pc.property_name}")'
                            )
                            downgrade_lines.append(
                                f'ctx.add_node_property_existence_constraint("{label}", "{pc.property_name}")'
                            )
    
    # Process relationship changes
    for rc in diff.relationship_changes:
        if rc.kind == "added":
            upgrade_lines.append(f"# TODO: Handle new relationship '{rc.new.rel_type}' (kgid={rc.kgid})")
            downgrade_lines.append(f"# TODO: Clean up relationship '{rc.new.rel_type}' (kgid={rc.kgid})")
        elif rc.kind == "removed":
            upgrade_lines.append(f"# TODO: Handle removed relationship '{rc.old.rel_type}' (kgid={rc.kgid})")
            downgrade_lines.append(f"# TODO: Restore relationship '{rc.old.rel_type}' (kgid={rc.kgid})")
        elif rc.kind == "modified":
            # Handle relationship property changes similarly to nodes
            rel_type = rc.new.rel_type if rc.new else rc.old.rel_type
            for pc in rc.property_changes:
                if pc.kind == "added":
                    upgrade_lines.append(
                        f'# TODO: Add property "{pc.property_name}" to relationship {rel_type}'
                    )
                elif pc.kind == "removed":
                    upgrade_lines.append(
                        f'# TODO: Remove property "{pc.property_name}" from relationship {rel_type}'
                    )
    
    # Format the code
    if upgrade_lines:
        upgrade_code = "    " + "\n    ".join(upgrade_lines)
    else:
        upgrade_code = "    pass"
    
    if downgrade_lines:
        downgrade_code = "    " + "\n    ".join(downgrade_lines)
    else:
        downgrade_code = "    pass"
    
    return upgrade_code, downgrade_code


def render_migration(
    revision_id: str,
    parent_id: str | None,
    description: str,
    schema_hash: str,
    rollback_safe: bool = True,
    diff_summary: str = "",
    diff: "SchemaDiff | None" = None,
) -> str:
    """Render a migration file from template."""
    now = datetime.now(timezone.utc).isoformat()

    # Generate operations from diff if available
    if diff is not None and diff.has_changes:
        upgrade_lines, downgrade_lines = generate_operations_from_diff(diff)
        # Prepend comment header showing what was detected
        if diff_summary:
            comment_header = _DIFF_COMMENT_HEADER
            for line in diff_summary.splitlines():
                comment_header += f"    # {line}\n"
            upgrade_lines = comment_header + "\n" + upgrade_lines
            downgrade_lines = comment_header + "\n" + downgrade_lines
    elif diff_summary:
        # Fallback to old behavior if only summary is provided
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
