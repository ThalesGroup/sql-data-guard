## Why

LLM-driven applications are increasingly interacting with graph databases like Neo4j using the Cypher query language. Currently, SQL Data Guard only validates relational SQL queries. Without Cypher support, organizations using Neo4j cannot safely leverage SQL Data Guard to enforce read-only access, node label (table) and property (column) whitelists, or row-level (node-level) attribute restrictions on LLM-generated graph queries, leaving them vulnerable to data exfiltration and tampering.

## What Changes

- Support `cypher` (or `neo4j`) as a valid dialect option across all interfaces (FastAPI rest endpoint, MCP Wrapper, and Dify plugin).
- Implement read-only safety enforcement by blocking mutating Cypher operations such as `CREATE`, `DELETE`, `DETACH DELETE`, `SET`, `REMOVE`, and `MERGE`.
- Verify query structures against standard whitelist configurations (where labels match "tables" and properties match "columns").
- Support property-value restrictions (equivalent to SQL WHERE restrictions) to ensure queries conform to row-level security constraints.

## Capabilities

### New Capabilities
- `cypher-guard`: Core safety validation, whitelist checking, mutation blocking, and restriction enforcement for the Cypher (Neo4j) query language.

### Modified Capabilities
<!-- Existing capabilities whose REQUIREMENTS are changing (not just implementation).
     Only list here if spec-level behavior changes. Each needs a delta spec file.
     Use existing spec names from openspec/specs/. Leave empty if no requirement changes. -->

## Impact

- **Core Library (`src/sql_data_guard/`)**: Add a new verification router or handler in `sql_data_guard.py` when the dialect is `cypher` or `neo4j`. Introduce a robust lightweight Cypher verifier module.
- **REST API (`src/sql_data_guard/rest/`)**: Support Cypher queries via the `/verify-sql` endpoint by passing `"cypher"` or `"neo4j"` as the dialect.
- **Tests**: Create `tests/test_cypher_guard_unit.py` containing comprehensive test scenarios for allowed/blocked labels, properties, restrictions, and mutation checks.
