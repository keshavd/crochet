"""Tests for configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from crochet.config import CrochetConfig, find_project_root, load_config
from crochet.errors import ProjectNotInitializedError


class TestConfig:
    def test_load_config(self, tmp_project: Path):
        config = load_config(tmp_project)
        assert config.project_name == "test-graph"
        assert config.models_path == "models"
        assert config.neo4j.uri == "bolt://localhost:7687"

    def test_find_project_root(self, tmp_project: Path):
        # Create a subdirectory and search from there
        sub = tmp_project / "a" / "b"
        sub.mkdir(parents=True)
        root = find_project_root(sub)
        assert root == tmp_project

    def test_find_project_root_not_found(self, tmp_path: Path):
        with pytest.raises(ProjectNotInitializedError):
            find_project_root(tmp_path)

    def test_save_and_reload(self, tmp_path: Path):
        config = CrochetConfig(project_name="saved", project_root=tmp_path)
        config.save()
        loaded = load_config(tmp_path)
        assert loaded.project_name == "saved"

    def test_paths(self, tmp_project: Path):
        config = load_config(tmp_project)
        assert config.models_dir == tmp_project / "models"
        assert config.migrations_dir == tmp_project / "migrations"
        assert config.ledger_file == tmp_project / ".crochet" / "ledger.db"
