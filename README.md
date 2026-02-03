# Crochet

Versioned schema & data migrations for [neomodel](https://github.com/neo4j-contrib/neomodel) Neo4j graphs.

Crochet is a Git-backed, migration-driven framework that makes neomodel-defined
Neo4j graphs evolvable, auditable, and rollback-safe without relying on database
introspection.

## Problem It Solves

- neomodel has no native schema diff or migration system
- Neo4j is schemaless, so schema drift is silent
- Data loading and schema evolution are often intertwined but unmanaged
- Rollbacks are usually impossible or unsafe
- Git history and database state frequently diverge

Crochet enforces alignment between neomodel code, data ingests, and the live
graph.

## Installation

```bash
pip install crochet
```

For development:

```bash
pip install -e ".[dev]"
```

## Quick Start

### 1. Initialize a project

```bash
crochet new-project --name my-graph
```

This creates:

```
my-graph/
  crochet.toml          # project config
  models/               # neomodel definitions
  migrations/           # migration files
  .crochet/ledger.db    # SQLite ledger
```

### 2. Create node and relationship models

```bash
crochet create-node Person
crochet create-relationship Friendship --rel-type FRIENDS_WITH
```

Each model gets an immutable `__kgid__` identifier. Models can be renamed or
moved across files without losing identity, because the `__kgid__` is what
Crochet tracks — not class names or file paths.

```python
# models/person.py
from neomodel import StructuredNode, StringProperty, IntegerProperty

class Person(StructuredNode):
    __kgid__ = "person_v1"
    name = StringProperty(required=True, unique_index=True)
    age = IntegerProperty(index=True)
```

### 3. Create a migration

```bash
crochet create-migration "add person node"
```

Crochet snapshots the current schema IR, diffs it against the previous
snapshot, and scaffolds a migration file with detected changes as comments:

```python
# migrations/0001_add_person_node.py

revision_id = "0001_add_person_node"
parent_id = None
schema_hash = "a1b2c3..."
rollback_safe = True

def upgrade(ctx):
    ctx.add_unique_constraint("Person", "name")
    ctx.add_index("Person", "age")

def downgrade(ctx):
    ctx.drop_index("Person", "age")
    ctx.drop_unique_constraint("Person", "name")
```

### 4. Apply migrations

```bash
crochet upgrade              # apply all pending
crochet upgrade --dry-run    # preview without executing
crochet upgrade --target 0001_add_person_node  # apply up to a specific revision
```

### 5. Revert migrations

```bash
crochet downgrade            # revert the most recent migration
crochet downgrade --target 0001_add_person_node  # revert down to a target
```

Rollback-unsafe migrations will refuse to downgrade and raise an error.

### 6. Check status and verify

```bash
crochet status     # show applied/pending migrations, head, batches
crochet verify     # check ledger chain, file presence, schema hash consistency
```

## Core Concepts

### Intermediate Representation (IR)

neomodel files are parsed into an intermediate schema representation. IR
snapshots can be hashed, serialized, and diffed. No Neo4j connection is
required for schema comparison.

### Hash-Chained Migrations

Migrations are ordered by a parent chain (Alembic-style). Each migration
records the schema hash at the time it was created, so drift between code and
migrations is detectable.

### SQLite Ledger

A local SQLite database (`.crochet/ledger.db`) is the authoritative record of:

- Applied migrations and their order
- Dataset batches with file checksums and loader versions
- Schema snapshots for diffing

### Deterministic Data Ingest

Data loading is a first-class migration operation. The `MigrationContext`
provides helpers for batch-tracked ingests:

```python
def upgrade(ctx):
    batch_id = ctx.begin_batch()
    ctx.create_nodes("Person", [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ])
```

Every node and relationship created through a batch is tagged with
`_crochet_batch`, enabling delete-by-batch rollback.

### Rollback Semantics

Rollbacks are explicitly declared, not assumed:

- Append-only ingests support `delete_nodes_by_batch` / `delete_relationships_by_batch`
- Destructive transforms must set `rollback_safe = False`
- Unsafe downgrades are prevented by policy

## Migration Context Operations

The `MigrationContext` passed to `upgrade()` and `downgrade()` provides:

| Operation | Description |
|-----------|-------------|
| `add_unique_constraint(label, prop)` | Create a uniqueness constraint |
| `drop_unique_constraint(label, prop)` | Drop a uniqueness constraint |
| `add_node_property_existence_constraint(label, prop)` | Create a NOT NULL constraint |
| `drop_node_property_existence_constraint(label, prop)` | Drop a NOT NULL constraint |
| `add_index(label, prop)` | Create an index |
| `drop_index(label, prop)` | Drop an index |
| `rename_label(old, new)` | Rename a node label |
| `rename_relationship_type(old, new)` | Rename a relationship type |
| `add_node_property(label, prop, default)` | Add a property with optional default |
| `remove_node_property(label, prop)` | Remove a property |
| `rename_node_property(label, old, new)` | Rename a property |
| `run_cypher(cypher, params)` | Execute raw Cypher |
| `begin_batch(batch_id)` | Start a tracked data batch |
| `create_nodes(label, data)` | Batch-create nodes |
| `create_relationships(src, tgt, type, data)` | Batch-create relationships |
| `delete_nodes_by_batch(label, batch_id)` | Delete nodes by batch |
| `delete_relationships_by_batch(type, batch_id)` | Delete relationships by batch |

## Configuration

`crochet.toml`:

```toml
[project]
name = "my-graph"
models_path = "models"
migrations_path = "migrations"

[neo4j]
uri = "bolt://localhost:7687"
username = "neo4j"

[ledger]
path = ".crochet/ledger.db"
```

Neo4j credentials can be overridden with environment variables:

- `CROCHET_NEO4J_URI`
- `CROCHET_NEO4J_USERNAME`
- `CROCHET_NEO4J_PASSWORD`

## CLI Reference

| Command | Description |
|---------|-------------|
| `crochet new-project` | Initialize a new Crochet project |
| `crochet create-node NAME` | Scaffold a StructuredNode model |
| `crochet create-relationship NAME` | Scaffold a StructuredRel model |
| `crochet create-migration DESC` | Create a new migration file |
| `crochet upgrade` | Apply pending migrations |
| `crochet downgrade` | Revert the most recent migration |
| `crochet status` | Show migration status |
| `crochet verify` | Run verification checks |

## Design Principles

- **No hidden magic** — all changes are explicit migration files
- **Code > database state** — neomodel files are the source of truth
- **Determinism over convenience** — schema IR is hashed and diffed
- **Rollback is a contract, not a guess** — explicitly declared per migration
- **Git history and graph state must agree** — ledger + hash chains enforce this

## Development

```bash
pip install -e ".[dev]"
pytest
```

## License

MIT
