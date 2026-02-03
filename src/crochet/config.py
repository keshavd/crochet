"""Project configuration for Crochet."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import toml

from crochet.errors import ConfigError, ProjectNotInitializedError

CONFIG_FILENAME = "crochet.toml"
DEFAULT_MODELS_PATH = "models"
DEFAULT_MIGRATIONS_PATH = "migrations"
DEFAULT_LEDGER_PATH = ".crochet/ledger.db"


@dataclass
class Neo4jConfig:
    uri: str = "bolt://localhost:7687"
    username: str = "neo4j"
    password: str = ""

    def __post_init__(self) -> None:
        self.uri = os.environ.get("CROCHET_NEO4J_URI", self.uri)
        self.username = os.environ.get("CROCHET_NEO4J_USERNAME", self.username)
        self.password = os.environ.get("CROCHET_NEO4J_PASSWORD", self.password)


@dataclass
class CrochetConfig:
    project_name: str = "my-graph"
    models_path: str = DEFAULT_MODELS_PATH
    migrations_path: str = DEFAULT_MIGRATIONS_PATH
    ledger_path: str = DEFAULT_LEDGER_PATH
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    project_root: Path = field(default_factory=lambda: Path.cwd())

    @property
    def models_dir(self) -> Path:
        return self.project_root / self.models_path

    @property
    def migrations_dir(self) -> Path:
        return self.project_root / self.migrations_path

    @property
    def ledger_file(self) -> Path:
        return self.project_root / self.ledger_path

    def to_dict(self) -> dict:
        return {
            "project": {
                "name": self.project_name,
                "models_path": self.models_path,
                "migrations_path": self.migrations_path,
            },
            "neo4j": {
                "uri": self.neo4j.uri,
                "username": self.neo4j.username,
            },
            "ledger": {
                "path": self.ledger_path,
            },
        }

    def save(self, path: Path | None = None) -> None:
        target = path or (self.project_root / CONFIG_FILENAME)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "w") as f:
            toml.dump(self.to_dict(), f)


def find_project_root(start: Path | None = None) -> Path:
    """Walk up from *start* looking for crochet.toml."""
    current = (start or Path.cwd()).resolve()
    while True:
        if (current / CONFIG_FILENAME).exists():
            return current
        parent = current.parent
        if parent == current:
            raise ProjectNotInitializedError(str(start or Path.cwd()))
        current = parent


def load_config(project_root: Path | None = None) -> CrochetConfig:
    """Load and return the project configuration."""
    root = project_root or find_project_root()
    config_path = root / CONFIG_FILENAME
    if not config_path.exists():
        raise ProjectNotInitializedError(str(root))

    try:
        data = toml.load(config_path)
    except Exception as exc:
        raise ConfigError(f"Failed to parse {config_path}: {exc}") from exc

    proj = data.get("project", {})
    neo = data.get("neo4j", {})
    ledger = data.get("ledger", {})

    neo4j_config = Neo4jConfig(
        uri=neo.get("uri", "bolt://localhost:7687"),
        username=neo.get("username", "neo4j"),
        password=neo.get("password", ""),
    )

    return CrochetConfig(
        project_name=proj.get("name", "my-graph"),
        models_path=proj.get("models_path", DEFAULT_MODELS_PATH),
        migrations_path=proj.get("migrations_path", DEFAULT_MIGRATIONS_PATH),
        ledger_path=ledger.get("path", DEFAULT_LEDGER_PATH),
        neo4j=neo4j_config,
        project_root=root,
    )
