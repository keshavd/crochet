"""Scaffold a new neomodel StructuredNode file with an immutable __kgid__."""

from __future__ import annotations

import uuid
from pathlib import Path

_NODE_TEMPLATE = '''\
"""Node model: {class_name}"""

from neomodel import StructuredNode, StringProperty


class {class_name}(StructuredNode):
    """Graph node representing a {class_name}.

    The __kgid__ is an immutable identifier for this model's schema identity.
    It must never change, even if the class or file is renamed.
    """

    __kgid__ = "{kgid}"

    # -- Properties --
    name = StringProperty(required=True, unique_index=True)
'''


def scaffold_node(
    models_dir: Path,
    class_name: str,
    kgid: str | None = None,
    filename: str | None = None,
) -> Path:
    """Write a new node model file and return the path."""
    models_dir.mkdir(parents=True, exist_ok=True)

    # Ensure __init__.py
    init_path = models_dir / "__init__.py"
    if not init_path.exists():
        init_path.write_text("")

    kgid = kgid or f"{class_name.lower()}_{uuid.uuid4().hex[:8]}"
    fname = filename or f"{class_name.lower()}.py"
    file_path = models_dir / fname

    content = _NODE_TEMPLATE.format(class_name=class_name, kgid=kgid)
    file_path.write_text(content)
    return file_path
