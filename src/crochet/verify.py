"""Verification logic — ensure ledger, migrations, and graph agree."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from crochet.config import CrochetConfig
from crochet.errors import VerificationError
from crochet.ledger.sqlite import Ledger
from crochet.migrations.engine import MigrationEngine


@dataclass
class VerificationReport:
    """Result of a verification run."""

    checks: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def summary(self) -> str:
        lines: list[str] = []
        for c in self.checks:
            icon = "PASS" if c.passed else "FAIL"
            lines.append(f"[{icon}] {c.name}")
            if c.details:
                for d in c.details:
                    lines.append(f"       {d}")
        return "\n".join(lines)


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: list[str] = field(default_factory=list)


def verify_project(
    config: CrochetConfig,
    ledger: Ledger,
    driver: Any | None = None,
) -> VerificationReport:
    """Run all verification checks and return a report."""
    report = VerificationReport()

    # 1. Ledger chain integrity
    report.checks.append(_check_ledger_chain(ledger))

    # 2. Migration files match ledger
    engine = MigrationEngine(config, ledger)
    report.checks.append(_check_migration_files_match_ledger(engine, ledger))

    # 3. No pending migrations
    report.checks.append(_check_no_pending(engine))

    # 4. Schema hash consistency
    report.checks.append(_check_schema_hashes(engine, ledger))

    # 5. Neo4j connectivity (if driver provided)
    if driver is not None:
        report.checks.append(_check_neo4j_connectivity(driver))

    return report


def _check_ledger_chain(ledger: Ledger) -> CheckResult:
    issues = ledger.verify_chain()
    if issues:
        return CheckResult(
            name="Ledger chain integrity",
            passed=False,
            details=issues,
        )
    return CheckResult(name="Ledger chain integrity", passed=True)


def _check_migration_files_match_ledger(
    engine: MigrationEngine, ledger: Ledger
) -> CheckResult:
    """Every applied migration in the ledger must have a corresponding file."""
    applied = ledger.get_applied_migrations()
    discovered = {m.revision_id for m in engine.discover_migrations()}
    missing: list[str] = []
    for am in applied:
        if am.revision_id not in discovered:
            missing.append(f"Ledger references '{am.revision_id}' but no file found.")
    if missing:
        return CheckResult(
            name="Migration files present",
            passed=False,
            details=missing,
        )
    return CheckResult(name="Migration files present", passed=True)


def _check_no_pending(engine: MigrationEngine) -> CheckResult:
    pending = engine.pending_migrations()
    if pending:
        return CheckResult(
            name="No pending migrations",
            passed=False,
            details=[f"Pending: {m.revision_id}" for m in pending],
        )
    return CheckResult(name="No pending migrations", passed=True)


def _check_schema_hashes(engine: MigrationEngine, ledger: Ledger) -> CheckResult:
    """Check that schema hashes in migration files match the ledger."""
    applied = {m.revision_id: m for m in ledger.get_applied_migrations()}
    issues: list[str] = []
    for mf in engine.discover_migrations():
        am = applied.get(mf.revision_id)
        if am and mf.schema_hash and am.schema_hash != mf.schema_hash:
            issues.append(
                f"Hash mismatch for '{mf.revision_id}': "
                f"file={mf.schema_hash[:12]}… ledger={am.schema_hash[:12]}…"
            )
    if issues:
        return CheckResult(
            name="Schema hash consistency",
            passed=False,
            details=issues,
        )
    return CheckResult(name="Schema hash consistency", passed=True)


def _check_neo4j_connectivity(driver: Any) -> CheckResult:
    try:
        with driver.session() as session:
            session.run("RETURN 1")
        return CheckResult(name="Neo4j connectivity", passed=True)
    except Exception as exc:
        return CheckResult(
            name="Neo4j connectivity",
            passed=False,
            details=[str(exc)],
        )
