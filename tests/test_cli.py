"""Tests for the CLI interface."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from crochet.cli import main


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def project_dir(tmp_path: Path, runner: CliRunner) -> Path:
    """Create a crochet project via the CLI."""
    result = runner.invoke(main, ["new-project", "--name", "test", "--path", str(tmp_path)])
    assert result.exit_code == 0, result.output
    return tmp_path


class TestNewProject:
    def test_creates_structure(self, runner: CliRunner, tmp_path: Path):
        result = runner.invoke(
            main, ["new-project", "--name", "my-graph", "--path", str(tmp_path)]
        )
        assert result.exit_code == 0
        assert (tmp_path / "crochet.toml").exists()
        assert (tmp_path / "models").is_dir()
        assert (tmp_path / "migrations").is_dir()
        assert (tmp_path / ".crochet" / "ledger.db").exists()
        assert "Initialized" in result.output


class TestCreateNode:
    def test_scaffolds_node(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        result = runner.invoke(main, ["create-node", "Person"])
        assert result.exit_code == 0, result.output
        assert "Created node model" in result.output
        assert (project_dir / "models" / "person.py").exists()

    def test_with_kgid(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        result = runner.invoke(main, ["create-node", "Person", "--kgid", "person_v1"])
        assert result.exit_code == 0
        content = (project_dir / "models" / "person.py").read_text()
        assert "person_v1" in content


class TestCreateRelationship:
    def test_scaffolds_rel(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        result = runner.invoke(main, ["create-relationship", "Friendship"])
        assert result.exit_code == 0, result.output
        assert "Created relationship model" in result.output

    def test_with_rel_type(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        result = runner.invoke(
            main,
            ["create-relationship", "Friendship", "--rel-type", "FRIENDS_WITH"],
        )
        assert result.exit_code == 0
        content = (project_dir / "models" / "friendship.py").read_text()
        assert "FRIENDS_WITH" in content


class TestCreateMigration:
    def test_creates_migration(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        result = runner.invoke(main, ["create-migration", "initial setup", "--no-snapshot"])
        assert result.exit_code == 0, result.output
        assert "Created migration" in result.output
        files = list((project_dir / "migrations").glob("0001_*.py"))
        assert len(files) == 1

    def test_migration_with_snapshot(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        # Create a node first
        runner.invoke(main, ["create-node", "Person", "--kgid", "person_v1"])
        result = runner.invoke(main, ["create-migration", "add person"])
        assert result.exit_code == 0, result.output
        assert "schema hash" in result.output


class TestUpgradeDowngrade:
    def _setup_migration(self, project_dir: Path) -> None:
        import textwrap

        content = textwrap.dedent("""\
            from crochet.migrations.operations import MigrationContext
            revision_id = "0001_init"
            parent_id = None
            schema_hash = "abc"
            rollback_safe = True
            def upgrade(ctx: MigrationContext) -> None:
                pass
            def downgrade(ctx: MigrationContext) -> None:
                pass
        """)
        (project_dir / "migrations" / "0001_init.py").write_text(content)

    def test_upgrade(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        self._setup_migration(project_dir)
        result = runner.invoke(main, ["upgrade", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "Applied: 0001_init" in result.output

    def test_upgrade_nothing_pending(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        result = runner.invoke(main, ["upgrade"])
        assert result.exit_code == 0
        assert "up to date" in result.output

    def test_downgrade(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        self._setup_migration(project_dir)
        runner.invoke(main, ["upgrade"])
        result = runner.invoke(main, ["downgrade", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "Reverted: 0001_init" in result.output


class TestStatus:
    def test_status_empty(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "test" in result.output  # project name
        assert "Total migrations" in result.output

    def test_status_with_migrations(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        runner.invoke(main, ["create-migration", "first", "--no-snapshot"])
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "Pending" in result.output


class TestVerify:
    def test_verify_clean(self, runner: CliRunner, project_dir: Path):
        os.chdir(project_dir)
        result = runner.invoke(main, ["verify"])
        assert result.exit_code == 0
        assert "PASS" in result.output
