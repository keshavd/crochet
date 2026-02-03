"""Diff two schema snapshots to produce migration intent."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crochet.ir.schema import NodeIR, PropertyIR, RelationshipIR, SchemaSnapshot


@dataclass
class PropertyChange:
    """A single property-level change."""

    kind: str  # "added", "removed", "modified"
    property_name: str
    old: "PropertyIR | None" = None
    new: "PropertyIR | None" = None

    @property
    def description(self) -> str:
        if self.kind == "added":
            return f"  + property '{self.property_name}' ({self.new.property_type})"  # type: ignore[union-attr]
        elif self.kind == "removed":
            return f"  - property '{self.property_name}'"
        else:
            changes = []
            if self.old and self.new:
                if self.old.property_type != self.new.property_type:
                    changes.append(
                        f"type {self.old.property_type} -> {self.new.property_type}"
                    )
                if self.old.required != self.new.required:
                    changes.append(f"required={self.new.required}")
                if self.old.unique_index != self.new.unique_index:
                    changes.append(f"unique_index={self.new.unique_index}")
                if self.old.index != self.new.index:
                    changes.append(f"index={self.new.index}")
            detail = ", ".join(changes) or "modified"
            return f"  ~ property '{self.property_name}' ({detail})"


@dataclass
class NodeChange:
    """Change descriptor for a node."""

    kind: str  # "added", "removed", "modified"
    kgid: str
    old: "NodeIR | None" = None
    new: "NodeIR | None" = None
    property_changes: list[PropertyChange] = field(default_factory=list)
    label_renamed: bool = False

    @property
    def description(self) -> str:
        if self.kind == "added":
            return f"+ Node '{self.new.label}' (kgid={self.kgid})"  # type: ignore[union-attr]
        elif self.kind == "removed":
            return f"- Node '{self.old.label}' (kgid={self.kgid})"  # type: ignore[union-attr]
        else:
            parts = [f"~ Node kgid={self.kgid}"]
            if self.label_renamed:
                parts.append(
                    f"  renamed '{self.old.label}' -> '{self.new.label}'"  # type: ignore[union-attr]
                )
            for pc in self.property_changes:
                parts.append(pc.description)
            return "\n".join(parts)


@dataclass
class RelationshipChange:
    """Change descriptor for a relationship model."""

    kind: str
    kgid: str
    old: "RelationshipIR | None" = None
    new: "RelationshipIR | None" = None
    property_changes: list[PropertyChange] = field(default_factory=list)

    @property
    def description(self) -> str:
        if self.kind == "added":
            return f"+ Relationship '{self.new.rel_type}' (kgid={self.kgid})"  # type: ignore[union-attr]
        elif self.kind == "removed":
            return f"- Relationship '{self.old.rel_type}' (kgid={self.kgid})"  # type: ignore[union-attr]
        else:
            parts = [f"~ Relationship kgid={self.kgid}"]
            for pc in self.property_changes:
                parts.append(pc.description)
            return "\n".join(parts)


@dataclass
class SchemaDiff:
    """Full diff between two schema snapshots."""

    node_changes: list[NodeChange] = field(default_factory=list)
    relationship_changes: list[RelationshipChange] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.node_changes or self.relationship_changes)

    def summary(self) -> str:
        if not self.has_changes:
            return "No schema changes detected."
        lines: list[str] = []
        for nc in self.node_changes:
            lines.append(nc.description)
        for rc in self.relationship_changes:
            lines.append(rc.description)
        return "\n".join(lines)


def _diff_properties(
    old_props: tuple["PropertyIR", ...],
    new_props: tuple["PropertyIR", ...],
) -> list[PropertyChange]:
    """Diff two sets of properties by name."""
    old_map = {p.name: p for p in old_props}
    new_map = {p.name: p for p in new_props}
    changes: list[PropertyChange] = []

    for name in sorted(set(old_map) | set(new_map)):
        old_p = old_map.get(name)
        new_p = new_map.get(name)
        if old_p is None and new_p is not None:
            changes.append(PropertyChange(kind="added", property_name=name, new=new_p))
        elif old_p is not None and new_p is None:
            changes.append(PropertyChange(kind="removed", property_name=name, old=old_p))
        elif old_p != new_p:
            changes.append(
                PropertyChange(kind="modified", property_name=name, old=old_p, new=new_p)
            )

    return changes


def diff_snapshots(
    old: "SchemaSnapshot", new: "SchemaSnapshot"
) -> SchemaDiff:
    """Compute a SchemaDiff between two snapshots keyed by __kgid__."""
    diff = SchemaDiff()

    old_nodes = old.nodes_by_kgid
    new_nodes = new.nodes_by_kgid

    for kgid in sorted(set(old_nodes) | set(new_nodes)):
        old_n = old_nodes.get(kgid)
        new_n = new_nodes.get(kgid)
        if old_n is None and new_n is not None:
            diff.node_changes.append(NodeChange(kind="added", kgid=kgid, new=new_n))
        elif old_n is not None and new_n is None:
            diff.node_changes.append(NodeChange(kind="removed", kgid=kgid, old=old_n))
        elif old_n != new_n:
            prop_changes = _diff_properties(old_n.properties, new_n.properties)  # type: ignore[union-attr]
            label_renamed = old_n.label != new_n.label  # type: ignore[union-attr]
            if prop_changes or label_renamed:
                diff.node_changes.append(
                    NodeChange(
                        kind="modified",
                        kgid=kgid,
                        old=old_n,
                        new=new_n,
                        property_changes=prop_changes,
                        label_renamed=label_renamed,
                    )
                )

    old_rels = old.relationships_by_kgid
    new_rels = new.relationships_by_kgid

    for kgid in sorted(set(old_rels) | set(new_rels)):
        old_r = old_rels.get(kgid)
        new_r = new_rels.get(kgid)
        if old_r is None and new_r is not None:
            diff.relationship_changes.append(
                RelationshipChange(kind="added", kgid=kgid, new=new_r)
            )
        elif old_r is not None and new_r is None:
            diff.relationship_changes.append(
                RelationshipChange(kind="removed", kgid=kgid, old=old_r)
            )
        elif old_r != new_r:
            prop_changes = _diff_properties(old_r.properties, new_r.properties)  # type: ignore[union-attr]
            if prop_changes:
                diff.relationship_changes.append(
                    RelationshipChange(
                        kind="modified",
                        kgid=kgid,
                        old=old_r,
                        new=new_r,
                        property_changes=prop_changes,
                    )
                )

    return diff
