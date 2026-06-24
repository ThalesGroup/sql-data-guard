## Context

With SQL Data Guard becoming the standard safety layer for LLM-to-database interactions, there is a growing demand to support graph databases like Neo4j. Since Neo4j uses the Cypher query language rather than SQL, SQL Data Guard's underlying parsing engine (`sqlglot`) is not capable of parsing these queries. This design proposes a clean, lightweight, and robust Cypher validation system that maps Cypher concepts directly onto the existing SQL Data Guard configuration schemas.

## Goals / Non-Goals

**Goals:**
- Detect `cypher` and `neo4j` dialects and route them to a dedicated Cypher verifier.
- Prevent query mutation attacks by blocking clauses such as `CREATE`, `DELETE`, `DETACH`, `SET`, `REMOVE`, and `MERGE`.
- Extract and validate all node labels and relationship types in a query, treating them as table names.
- Extract and validate all node/relationship property keys accessed in the query, treating them as columns.
- Support basic property value restrictions to enforce row-level security.
- Maintain zero new external dependencies to ensure full compatibility and minimal package size.

**Non-Goals:**
- Build a full-featured Cypher AST rewriter; unlike SQL, fixing/rewriting Cypher queries is out of scope. Validation will be strictly allowed or blocked.
- Support complex graph schema validation (e.g., validating multi-label paths or deep traversal security properties).

## Decisions

### Decision 1: Custom Regex-based Scanner vs. External Cypher Parsers
- **Choice:** Custom Regex-based tokenizer/scanner.
- **Rationale:** Cypher query structures (labels, properties, clauses) can be accurately identified and validated using pattern matching. Standard third-party Cypher parsers either bring heavy dependencies (e.g., ANTLR) or lack support/maintenance for Python 3.13. A custom, optimized parser module avoids dependency bloat and ensures long-term maintainability.
- **Alternatives Considered:**
  - **ANTLR4 Cypher Parser:** Rejected due to excessive dependency size and compile-time overhead.
  - **Pypher / openCypher libraries:** Rejected as they are unmaintained or fail on modern Python runtimes.

### Decision 2: Interface Mapping
- **Choice:** Direct mapping of Cypher syntax to SQL schema configuration structure.
  - Node Labels & Relationship Types $\rightarrow$ Configured `table_name`
  - Properties $\rightarrow$ Configured `columns`
  - Node/Relationship Property Restrictions $\rightarrow$ Configured `restrictions`
- **Rationale:** Reuses the exact same configuration schema already defined in SQL Data Guard, preventing the need to change any config-loading or API validation layers.

## Risks / Trade-offs

- **[Risk] Complex Cypher Queries bypassing Regex:** Highly nested or obfuscated Cypher queries could theoretically hide labels or property keys from naive regex matchers.
  - *Mitigation:* Use a defensive tokenizer to strip comments, string literals, and check syntax systematically. If the scanner cannot cleanly parse or identify components, the query is rejected (fail-secure).
- **[Risk] Dialect Routing Overhead:**
  - *Mitigation:* Implement simple routing at the top level of `verify_sql` so that standard SQL queries bypass any Cypher parsing logic entirely.
