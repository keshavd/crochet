"""Tests for node and relationship scaffolding."""

from __future__ import annotations

from pathlib import Path

from crochet.scaffold.node import scaffold_node
from crochet.scaffold.relationship import scaffold_relationship


class TestScaffoldNode:
    def test_creates_file(self, tmp_path: Path):
        models_dir = tmp_path / "models"
        path = scaffold_node(models_dir, "Person")
        assert path.exists()
        assert path.name == "person.py"
        content = path.read_text()
        assert "class Person(StructuredNode)" in content
        assert "__kgid__" in content
        assert "StringProperty" in content

    def test_custom_kgid(self, tmp_path: Path):
        models_dir = tmp_path / "models"
        path = scaffold_node(models_dir, "Person", kgid="my_person_id")
        content = path.read_text()
        assert 'my_person_id' in content

    def test_custom_filename(self, tmp_path: Path):
        models_dir = tmp_path / "models"
        path = scaffold_node(models_dir, "Person", filename="people.py")
        assert path.name == "people.py"

    def test_creates_init(self, tmp_path: Path):
        models_dir = tmp_path / "models"
        scaffold_node(models_dir, "X")
        assert (models_dir / "__init__.py").exists()


class TestScaffoldRelationship:
    def test_creates_file(self, tmp_path: Path):
        models_dir = tmp_path / "models"
        path = scaffold_relationship(models_dir, "Friendship")
        assert path.exists()
        content = path.read_text()
        assert "class Friendship(StructuredRel)" in content
        assert "__kgid__" in content
        assert "__type__" in content

    def test_custom_rel_type(self, tmp_path: Path):
        models_dir = tmp_path / "models"
        path = scaffold_relationship(
            models_dir, "Friendship", rel_type="FRIENDS_WITH"
        )
        content = path.read_text()
        assert "FRIENDS_WITH" in content

    def test_default_rel_type(self, tmp_path: Path):
        models_dir = tmp_path / "models"
        path = scaffold_relationship(models_dir, "Knows")
        content = path.read_text()
        assert "KNOWS" in content
