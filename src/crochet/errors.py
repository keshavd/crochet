"""Custom exceptions for the Crochet framework."""


class CrochetError(Exception):
    """Base exception for all Crochet errors."""


class ProjectNotInitializedError(CrochetError):
    """Raised when a crochet command is run outside an initialized project."""

    def __init__(self, path: str = "."):
        super().__init__(
            f"No crochet project found at '{path}'. Run 'crochet new-project' first."
        )


class ConfigError(CrochetError):
    """Raised for configuration file issues."""


class SchemaError(CrochetError):
    """Raised for schema parsing or validation issues."""


class MissingKGIDError(SchemaError):
    """Raised when a neomodel class is missing a __kgid__."""

    def __init__(self, class_name: str):
        super().__init__(
            f"Class '{class_name}' is missing a __kgid__ attribute. "
            "Every node and relationship model must declare an immutable __kgid__."
        )


class DuplicateKGIDError(SchemaError):
    """Raised when two classes share the same __kgid__."""

    def __init__(self, kgid: str, class1: str, class2: str):
        super().__init__(
            f"Duplicate __kgid__ '{kgid}' found on classes '{class1}' and '{class2}'."
        )


class MigrationError(CrochetError):
    """Raised for migration execution issues."""


class MigrationChainError(MigrationError):
    """Raised when the migration chain is broken or inconsistent."""


class RollbackUnsafeError(MigrationError):
    """Raised when attempting to downgrade a non-rollback-safe migration."""

    def __init__(self, revision_id: str):
        super().__init__(
            f"Migration '{revision_id}' is marked as rollback-unsafe. "
            "Downgrade is not permitted."
        )


class LedgerError(CrochetError):
    """Raised for SQLite ledger issues."""


class LedgerIntegrityError(LedgerError):
    """Raised when the ledger state is inconsistent."""


class IngestError(CrochetError):
    """Raised for data ingest issues."""


class RemoteFetchError(IngestError):
    """Raised when a remote file cannot be fetched."""


class ChecksumMismatchError(IngestError):
    """Raised when a downloaded file's checksum doesn't match the expected value."""

    def __init__(self, uri: str, expected: str, actual: str):
        super().__init__(
            f"Checksum mismatch for '{uri}': expected {expected}, got {actual}"
        )
        self.uri = uri
        self.expected = expected
        self.actual = actual


class VerificationError(CrochetError):
    """Raised when verification checks fail."""
