"""Scaffold a new neomodel StructuredRel file with an immutable __kgid__."""

from __future__ import annotations

import uuid
from pathlib import Path

_REL_TEMPLATE = '''\
"""Relationship model: {class_name}"""

from neomodel import StructuredRel, StringProperty


class {class_name}(StructuredRel):
    """Graph relationship representing a {class_name}.

    The __kgid__ is an immutable identifier for this model's schema identity.
    It must never change, even if the class or file is renamed.
    """

    __kgid__ = "{kgid}"
    __type__ = "{rel_type}"

    # -- Properties --
'''


def scaffold_relationship(
    models_dir: Path,
    class_name: str,
    rel_type: str | None = None,
    kgid: str | None = None,
    filename: str | None = None,
) -> Path:
    """Write a new relationship model file and return the path."""
    models_dir.mkdir(parents=True, exist_ok=True)

    # Ensure __init__.py
    init_path = models_dir / "__init__.py"
    if not init_path.exists():
        init_path.write_text("")

    kgid = kgid or f"{class_name.lower()}_{uuid.uuid4().hex[:8]}"
    rel_type = rel_type or class_name.upper()
    fname = filename or f"{class_name.lower()}.py"
    file_path = models_dir / fname

    content = _REL_TEMPLATE.format(
        class_name=class_name, kgid=kgid, rel_type=rel_type
    )
    file_path.write_text(content)
    return file_path
