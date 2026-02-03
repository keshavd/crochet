"""Tests for the IR module: schema structures, parsing, hashing, diffing."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from crochet.ir.schema import (
    NodeIR,
    PropertyIR,
    RelationshipDefIR,
    RelationshipIR,
    SchemaSnapshot,
)
from crochet.ir.hash import compute_hash, hash_snapshot
from crochet.ir.diff import diff_snapshots


# ======================================================================
# PropertyIR
# ======================================================================


class TestPropertyIR:
    def test_round_trip(self):
        p = PropertyIR(
            name="email",
            property_type="StringProperty",
            required=True,
            unique_index=True,
        )
        d = p.to_dict()
        p2 = PropertyIR.from_dict(d)
        assert p2 == p

    def test_ordering(self):
        a = PropertyIR(name="alpha", property_type="StringProperty")
        b = PropertyIR(name="beta", property_type="StringProperty")
        assert sorted([b, a]) == [a, b]


# ======================================================================
# NodeIR
# ======================================================================


class TestNodeIR:
    def test_round_trip(self):
        props = (
            PropertyIR(name="name", property_type="StringProperty", required=True),
            PropertyIR(name="age", property_type="IntegerProperty"),
        )
        rel_defs = (
            RelationshipDefIR(
                attr_name="friends",
                rel_type="FRIENDS_WITH",
                target_label="Person",
                direction="to",
            ),
        )
        node = NodeIR(
            kgid="person_v1",
            label="Person",
            class_name="Person",
            module_path="models.person",
            properties=props,
            relationship_defs=rel_defs,
        )
        d = node.to_dict()
        node2 = NodeIR.from_dict(d)
        assert node2.kgid == node.kgid
        assert node2.label == node.label
        assert len(node2.properties) == 2
        assert len(node2.relationship_defs) == 1


# ======================================================================
# SchemaSnapshot
# ======================================================================


class TestSchemaSnapshot:
    def test_empty_snapshot(self):
        s = SchemaSnapshot.empty()
        assert len(s.nodes) == 0
        assert len(s.relationships) == 0

    def test_json_round_trip(self):
        node = NodeIR(
            kgid="x_v1",
            label="X",
            class_name="X",
            module_path="m",
            properties=(
                PropertyIR(name="val", property_type="IntegerProperty"),
            ),
        )
        snap = SchemaSnapshot(nodes=(node,), relationships=())
        snap = hash_snapshot(snap)
        raw = snap.to_json()
        snap2 = SchemaSnapshot.from_json(raw)
        assert snap2.schema_hash == snap.schema_hash
        assert snap2.nodes[0].kgid == "x_v1"

    def test_nodes_by_kgid(self):
        n1 = NodeIR(kgid="a", label="A", class_name="A", module_path="m")
        n2 = NodeIR(kgid="b", label="B", class_name="B", module_path="m")
        snap = SchemaSnapshot(nodes=(n1, n2), relationships=())
        assert set(snap.nodes_by_kgid.keys()) == {"a", "b"}


# ======================================================================
# Hashing
# ======================================================================


class TestHashing:
    def test_deterministic(self):
        node = NodeIR(kgid="x", label="X", class_name="X", module_path="m")
        s1 = SchemaSnapshot(nodes=(node,), relationships=())
        s2 = SchemaSnapshot(nodes=(node,), relationships=())
        assert compute_hash(s1) == compute_hash(s2)

    def test_different_content_different_hash(self):
        n1 = NodeIR(kgid="x", label="X", class_name="X", module_path="m")
        n2 = NodeIR(
            kgid="x",
            label="X",
            class_name="X",
            module_path="m",
            properties=(PropertyIR(name="v", property_type="StringProperty"),),
        )
        s1 = SchemaSnapshot(nodes=(n1,), relationships=())
        s2 = SchemaSnapshot(nodes=(n2,), relationships=())
        assert compute_hash(s1) != compute_hash(s2)

    def test_created_at_ignored(self):
        node = NodeIR(kgid="x", label="X", class_name="X", module_path="m")
        s1 = SchemaSnapshot(nodes=(node,), relationships=(), created_at="2024-01-01")
        s2 = SchemaSnapshot(nodes=(node,), relationships=(), created_at="2025-12-31")
        assert compute_hash(s1) == compute_hash(s2)


# ======================================================================
# Diffing
# ======================================================================


class TestDiff:
    def _make_snap(self, nodes=(), rels=()):
        return SchemaSnapshot(nodes=tuple(nodes), relationships=tuple(rels))

    def test_no_changes(self):
        n = NodeIR(kgid="a", label="A", class_name="A", module_path="m")
        old = self._make_snap(nodes=[n])
        new = self._make_snap(nodes=[n])
        diff = diff_snapshots(old, new)
        assert not diff.has_changes

    def test_added_node(self):
        old = self._make_snap()
        n = NodeIR(kgid="a", label="A", class_name="A", module_path="m")
        new = self._make_snap(nodes=[n])
        diff = diff_snapshots(old, new)
        assert diff.has_changes
        assert len(diff.node_changes) == 1
        assert diff.node_changes[0].kind == "added"

    def test_removed_node(self):
        n = NodeIR(kgid="a", label="A", class_name="A", module_path="m")
        old = self._make_snap(nodes=[n])
        new = self._make_snap()
        diff = diff_snapshots(old, new)
        assert diff.has_changes
        assert diff.node_changes[0].kind == "removed"

    def test_modified_node_label_rename(self):
        n1 = NodeIR(kgid="a", label="OldName", class_name="A", module_path="m")
        n2 = NodeIR(kgid="a", label="NewName", class_name="A", module_path="m")
        diff = diff_snapshots(self._make_snap(nodes=[n1]), self._make_snap(nodes=[n2]))
        assert diff.has_changes
        assert diff.node_changes[0].kind == "modified"
        assert diff.node_changes[0].label_renamed is True

    def test_modified_node_property_added(self):
        n1 = NodeIR(kgid="a", label="A", class_name="A", module_path="m")
        n2 = NodeIR(
            kgid="a",
            label="A",
            class_name="A",
            module_path="m",
            properties=(PropertyIR(name="x", property_type="StringProperty"),),
        )
        diff = diff_snapshots(self._make_snap(nodes=[n1]), self._make_snap(nodes=[n2]))
        assert diff.has_changes
        change = diff.node_changes[0]
        assert change.kind == "modified"
        assert len(change.property_changes) == 1
        assert change.property_changes[0].kind == "added"

    def test_added_relationship(self):
        r = RelationshipIR(
            kgid="r1", rel_type="KNOWS", class_name="Knows", module_path="m"
        )
        diff = diff_snapshots(self._make_snap(), self._make_snap(rels=[r]))
        assert diff.has_changes
        assert diff.relationship_changes[0].kind == "added"

    def test_summary_text(self):
        n = NodeIR(kgid="a", label="A", class_name="A", module_path="m")
        diff = diff_snapshots(self._make_snap(), self._make_snap(nodes=[n]))
        summary = diff.summary()
        assert "Node" in summary
        assert "a" in summary


# ======================================================================
# Parser (requires neomodel importable)
# ======================================================================


class TestParser:
    def test_parse_node_file(self, tmp_project, sample_node_file):
        from crochet.ir.parser import parse_models_directory

        snap = parse_models_directory(tmp_project / "models")
        assert len(snap.nodes) == 1
        node = snap.nodes[0]
        assert node.kgid == "person_v1"
        assert node.label == "Person"
        # Properties
        prop_names = {p.name for p in node.properties}
        assert "name" in prop_names
        assert "age" in prop_names

    def test_parse_rel_file(self, tmp_project, sample_rel_file):
        from crochet.ir.parser import parse_models_directory

        snap = parse_models_directory(tmp_project / "models")
        assert len(snap.relationships) == 1
        rel = snap.relationships[0]
        assert rel.kgid == "friendship_v1"
        assert rel.rel_type == "FRIENDS_WITH"

    def test_missing_kgid_raises(self, tmp_project):
        content = textwrap.dedent("""\
            from neomodel import StructuredNode, StringProperty
            class BadNode(StructuredNode):
                name = StringProperty()
        """)
        (tmp_project / "models" / "bad.py").write_text(content)
        from crochet.ir.parser import parse_models_directory
        from crochet.errors import MissingKGIDError

        with pytest.raises(MissingKGIDError):
            parse_models_directory(tmp_project / "models")

    def test_duplicate_kgid_raises(self, tmp_project):
        file1 = textwrap.dedent("""\
            from neomodel import StructuredNode, StringProperty
            class Foo(StructuredNode):
                __kgid__ = "dupe"
                name = StringProperty()
        """)
        file2 = textwrap.dedent("""\
            from neomodel import StructuredNode, StringProperty
            class Bar(StructuredNode):
                __kgid__ = "dupe"
                name = StringProperty()
        """)
        (tmp_project / "models" / "foo.py").write_text(file1)
        (tmp_project / "models" / "bar.py").write_text(file2)
        from crochet.ir.parser import parse_models_directory
        from crochet.errors import DuplicateKGIDError

        with pytest.raises(DuplicateKGIDError):
            parse_models_directory(tmp_project / "models")

    def test_snapshot_has_hash(self, tmp_project, sample_node_file):
        from crochet.ir.parser import parse_models_directory

        snap = parse_models_directory(tmp_project / "models")
        assert snap.schema_hash
        assert len(snap.schema_hash) == 64  # SHA-256 hex length
