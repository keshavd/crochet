"""Parse neomodel Python files into schema IR without a Neo4j connection."""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import sys
from pathlib import Path
from typing import Any

from neomodel import (
    RelationshipFrom,
    RelationshipTo,
    StructuredNode,
    StructuredRel,
)
from neomodel.properties import Property

from crochet.errors import DuplicateKGIDError, MissingKGIDError
from crochet.ir.hash import hash_snapshot
from crochet.ir.schema import (
    NodeIR,
    PropertyIR,
    RelationshipDefIR,
    RelationshipIR,
    SchemaSnapshot,
)

# Relationship manager classes we recognise and their direction labels.
_REL_MANAGERS: dict[type, str] = {}


def _init_rel_managers() -> None:
    """Populate the relationship-manager mapping lazily."""
    if _REL_MANAGERS:
        return
    _REL_MANAGERS[RelationshipTo] = "to"
    _REL_MANAGERS[RelationshipFrom] = "from"
    try:
        from neomodel import Relationship

        _REL_MANAGERS[Relationship] = "either"
    except ImportError:
        pass


def _extract_property_ir(name: str, prop: Property) -> PropertyIR:
    """Turn a neomodel Property instance into a PropertyIR."""
    return PropertyIR(
        name=name,
        property_type=type(prop).__name__,
        required=getattr(prop, "required", False),
        unique_index=getattr(prop, "unique_index", False),
        index=getattr(prop, "index", False),
        default=None,  # we don't capture callables
        choices=tuple(prop.choices) if getattr(prop, "choices", None) else None,
    )


def _extract_rel_def_ir(attr_name: str, rel_mgr: Any) -> RelationshipDefIR | None:
    """Turn a neomodel relationship manager into a RelationshipDefIR."""
    _init_rel_managers()
    mgr_type = type(rel_mgr)
    direction = None
    for cls, dir_label in _REL_MANAGERS.items():
        if issubclass(mgr_type, cls):
            direction = dir_label
            break
    if direction is None:
        return None

    definition = rel_mgr.definition
    rel_type = definition.get("relation_type", "RELATED_TO")

    # Resolve the target model label
    target_cls = definition.get("node_class")
    if isinstance(target_cls, type) and issubclass(target_cls, StructuredNode):
        target_label = getattr(target_cls, "__label__", target_cls.__name__)
    else:
        target_label = str(target_cls) if target_cls else "UNKNOWN"

    # Check for a StructuredRel model
    model_cls = definition.get("model")
    model_kgid = None
    if model_cls is not None and isinstance(model_cls, type):
        model_kgid = getattr(model_cls, "__kgid__", None)

    return RelationshipDefIR(
        attr_name=attr_name,
        rel_type=rel_type,
        target_label=target_label,
        direction=direction,
        model_kgid=model_kgid,
    )


def _parse_node_class(cls: type) -> NodeIR:
    """Parse a single StructuredNode subclass into a NodeIR."""
    kgid = getattr(cls, "__kgid__", None)
    if kgid is None:
        raise MissingKGIDError(cls.__name__)

    label = getattr(cls, "__label__", cls.__name__)
    module_path = cls.__module__

    properties: list[PropertyIR] = []
    rel_defs: list[RelationshipDefIR] = []

    # Walk class attributes (not parent StructuredNode's)
    for attr_name in dir(cls):
        if attr_name.startswith("_"):
            continue
        try:
            attr = getattr(cls, attr_name)
        except Exception:
            continue

        if isinstance(attr, Property):
            properties.append(_extract_property_ir(attr_name, attr))
        else:
            rd = _extract_rel_def_ir(attr_name, attr)
            if rd is not None:
                rel_defs.append(rd)

    return NodeIR(
        kgid=kgid,
        label=label,
        class_name=cls.__name__,
        module_path=module_path,
        properties=tuple(sorted(properties)),
        relationship_defs=tuple(rel_defs),
    )


def _parse_rel_class(cls: type) -> RelationshipIR:
    """Parse a single StructuredRel subclass into a RelationshipIR."""
    kgid = getattr(cls, "__kgid__", None)
    if kgid is None:
        raise MissingKGIDError(cls.__name__)

    rel_type = getattr(cls, "__type__", cls.__name__.upper())
    module_path = cls.__module__

    properties: list[PropertyIR] = []
    for attr_name in dir(cls):
        if attr_name.startswith("_"):
            continue
        try:
            attr = getattr(cls, attr_name)
        except Exception:
            continue
        if isinstance(attr, Property):
            properties.append(_extract_property_ir(attr_name, attr))

    return RelationshipIR(
        kgid=kgid,
        rel_type=rel_type,
        class_name=cls.__name__,
        module_path=module_path,
        properties=tuple(sorted(properties)),
    )


def _clear_neomodel_registry() -> None:
    """Remove all entries from neomodel's class registry so re-imports work."""
    try:
        from neomodel import db

        if hasattr(db, "_NODE_CLASS_REGISTRY"):
            db._NODE_CLASS_REGISTRY.clear()
        if hasattr(db, "_DB_SPECIFIC_CLASS_REGISTRY"):
            db._DB_SPECIFIC_CLASS_REGISTRY.clear()
    except (ImportError, AttributeError):
        pass


_load_counter = 0


def _load_module_from_path(file_path: Path) -> Any:
    """Import a Python file as a module.

    Each call uses a unique module name so that neomodel's global class
    registry does not raise ``NodeClassAlreadyDefined`` on repeated parses
    of the same label across different directories or test runs.
    """
    global _load_counter
    _load_counter += 1
    module_name = f"crochet._user_models._{_load_counter}_{file_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        # If the module fails to import, skip it rather than crashing
        del sys.modules[module_name]
        raise
    return module


def parse_module(module: Any) -> tuple[list[NodeIR], list[RelationshipIR]]:
    """Extract all NodeIR and RelationshipIR from an already-loaded module."""
    nodes: list[NodeIR] = []
    rels: list[RelationshipIR] = []

    for _name, obj in inspect.getmembers(module, inspect.isclass):
        if obj.__module__ != module.__name__:
            continue  # skip imported classes
        if issubclass(obj, StructuredNode) and obj is not StructuredNode:
            nodes.append(_parse_node_class(obj))
        elif issubclass(obj, StructuredRel) and obj is not StructuredRel:
            rels.append(_parse_rel_class(obj))

    return nodes, rels


def parse_models_directory(models_dir: Path) -> SchemaSnapshot:
    """Parse all .py files in *models_dir* and return a SchemaSnapshot."""
    _clear_neomodel_registry()

    all_nodes: list[NodeIR] = []
    all_rels: list[RelationshipIR] = []
    seen_kgids: dict[str, str] = {}

    py_files = sorted(models_dir.rglob("*.py"))
    for py_file in py_files:
        if py_file.name.startswith("_"):
            continue
        module = _load_module_from_path(py_file)
        if module is None:
            continue
        nodes, rels = parse_module(module)

        for n in nodes:
            if n.kgid in seen_kgids:
                raise DuplicateKGIDError(n.kgid, seen_kgids[n.kgid], n.class_name)
            seen_kgids[n.kgid] = n.class_name
            all_nodes.append(n)

        for r in rels:
            if r.kgid in seen_kgids:
                raise DuplicateKGIDError(r.kgid, seen_kgids[r.kgid], r.class_name)
            seen_kgids[r.kgid] = r.class_name
            all_rels.append(r)

    snapshot = SchemaSnapshot(nodes=tuple(all_nodes), relationships=tuple(all_rels))
    return hash_snapshot(snapshot)
