"""Data structures for the schema Intermediate Representation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True, order=True)
class PropertyIR:
    """IR for a single property on a node or relationship."""

    name: str
    property_type: str  # e.g. "StringProperty", "IntegerProperty"
    required: bool = False
    unique_index: bool = False
    index: bool = False
    default: Any = None
    choices: tuple | None = None

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "name": self.name,
            "property_type": self.property_type,
            "required": self.required,
            "unique_index": self.unique_index,
            "index": self.index,
        }
        if self.default is not None:
            d["default"] = repr(self.default)
        if self.choices is not None:
            d["choices"] = list(self.choices)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> PropertyIR:
        return cls(
            name=d["name"],
            property_type=d["property_type"],
            required=d.get("required", False),
            unique_index=d.get("unique_index", False),
            index=d.get("index", False),
            default=d.get("default"),
            choices=tuple(d["choices"]) if d.get("choices") else None,
        )


@dataclass(frozen=True)
class RelationshipDefIR:
    """IR for a relationship definition on a node (e.g. RelationshipTo)."""

    attr_name: str
    rel_type: str  # Neo4j relationship type string, e.g. "FRIENDS_WITH"
    target_label: str  # target node class name or label
    direction: str  # "to", "from", "either"
    model_kgid: str | None = None  # __kgid__ of the StructuredRel model, if any

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "attr_name": self.attr_name,
            "rel_type": self.rel_type,
            "target_label": self.target_label,
            "direction": self.direction,
        }
        if self.model_kgid is not None:
            d["model_kgid"] = self.model_kgid
        return d

    @classmethod
    def from_dict(cls, d: dict) -> RelationshipDefIR:
        return cls(
            attr_name=d["attr_name"],
            rel_type=d["rel_type"],
            target_label=d["target_label"],
            direction=d["direction"],
            model_kgid=d.get("model_kgid"),
        )


@dataclass(frozen=True)
class NodeIR:
    """IR for a neomodel StructuredNode class."""

    kgid: str
    label: str  # Neo4j label (defaults to class name)
    class_name: str
    module_path: str
    properties: tuple[PropertyIR, ...] = ()
    relationship_defs: tuple[RelationshipDefIR, ...] = ()

    def to_dict(self) -> dict:
        return {
            "kgid": self.kgid,
            "label": self.label,
            "class_name": self.class_name,
            "module_path": self.module_path,
            "properties": [p.to_dict() for p in sorted(self.properties)],
            "relationship_defs": [r.to_dict() for r in self.relationship_defs],
        }

    @classmethod
    def from_dict(cls, d: dict) -> NodeIR:
        return cls(
            kgid=d["kgid"],
            label=d["label"],
            class_name=d["class_name"],
            module_path=d["module_path"],
            properties=tuple(PropertyIR.from_dict(p) for p in d.get("properties", [])),
            relationship_defs=tuple(
                RelationshipDefIR.from_dict(r) for r in d.get("relationship_defs", [])
            ),
        )


@dataclass(frozen=True)
class RelationshipIR:
    """IR for a neomodel StructuredRel class."""

    kgid: str
    rel_type: str  # Neo4j relationship type string
    class_name: str
    module_path: str
    properties: tuple[PropertyIR, ...] = ()

    def to_dict(self) -> dict:
        return {
            "kgid": self.kgid,
            "rel_type": self.rel_type,
            "class_name": self.class_name,
            "module_path": self.module_path,
            "properties": [p.to_dict() for p in sorted(self.properties)],
        }

    @classmethod
    def from_dict(cls, d: dict) -> RelationshipIR:
        return cls(
            kgid=d["kgid"],
            rel_type=d["rel_type"],
            class_name=d["class_name"],
            module_path=d["module_path"],
            properties=tuple(PropertyIR.from_dict(p) for p in d.get("properties", [])),
        )


@dataclass(frozen=True)
class SchemaSnapshot:
    """Immutable snapshot of the full schema IR at a point in time."""

    nodes: tuple[NodeIR, ...]
    relationships: tuple[RelationshipIR, ...]
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    schema_hash: str = ""

    @property
    def nodes_by_kgid(self) -> dict[str, NodeIR]:
        return {n.kgid: n for n in self.nodes}

    @property
    def relationships_by_kgid(self) -> dict[str, RelationshipIR]:
        return {r.kgid: r for r in self.relationships}

    def to_dict(self) -> dict:
        return {
            "nodes": [n.to_dict() for n in sorted(self.nodes, key=lambda n: n.kgid)],
            "relationships": [
                r.to_dict() for r in sorted(self.relationships, key=lambda r: r.kgid)
            ],
            "created_at": self.created_at,
            "schema_hash": self.schema_hash,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(cls, d: dict) -> SchemaSnapshot:
        return cls(
            nodes=tuple(NodeIR.from_dict(n) for n in d.get("nodes", [])),
            relationships=tuple(
                RelationshipIR.from_dict(r) for r in d.get("relationships", [])
            ),
            created_at=d.get("created_at", ""),
            schema_hash=d.get("schema_hash", ""),
        )

    @classmethod
    def from_json(cls, raw: str) -> SchemaSnapshot:
        return cls.from_dict(json.loads(raw))

    @classmethod
    def empty(cls) -> SchemaSnapshot:
        return cls(nodes=(), relationships=())
