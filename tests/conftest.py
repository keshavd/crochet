"""Shared test fixtures."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from crochet.config import CrochetConfig
from crochet.ledger.sqlite import Ledger


@pytest.fixture
def tmp_project(tmp_path: Path) -> Path:
    """Create a minimal crochet project layout in a temp directory."""
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").write_text("")

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    ledger_dir = tmp_path / ".crochet"
    ledger_dir.mkdir()

    config_content = textwrap.dedent(f"""\
        [project]
        name = "test-graph"
        models_path = "models"
        migrations_path = "migrations"

        [neo4j]
        uri = "bolt://localhost:7687"
        username = "neo4j"

        [ledger]
        path = ".crochet/ledger.db"
    """)
    (tmp_path / "crochet.toml").write_text(config_content)

    return tmp_path


@pytest.fixture
def config(tmp_project: Path) -> CrochetConfig:
    """Load a CrochetConfig from the temp project."""
    from crochet.config import load_config

    return load_config(tmp_project)


@pytest.fixture
def ledger(config: CrochetConfig) -> Ledger:
    """Open a ledger in the temp project."""
    led = Ledger(config.ledger_file)
    yield led
    led.close()


@pytest.fixture
def sample_node_file(tmp_project: Path) -> Path:
    """Write a sample neomodel node file into the models directory."""
    content = textwrap.dedent("""\
        from neomodel import StructuredNode, StringProperty, IntegerProperty

        class Person(StructuredNode):
            __kgid__ = "person_v1"
            name = StringProperty(required=True, unique_index=True)
            age = IntegerProperty(index=True)
    """)
    p = tmp_project / "models" / "person.py"
    p.write_text(content)
    return p


@pytest.fixture
def sample_rel_file(tmp_project: Path) -> Path:
    """Write a sample neomodel relationship file into the models directory."""
    content = textwrap.dedent("""\
        from neomodel import StructuredRel, StringProperty

        class Friendship(StructuredRel):
            __kgid__ = "friendship_v1"
            __type__ = "FRIENDS_WITH"
            since = StringProperty()
    """)
    p = tmp_project / "models" / "friendship.py"
    p.write_text(content)
    return p
