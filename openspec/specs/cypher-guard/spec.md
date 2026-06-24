# cypher-guard Specification

## Purpose
TBD - created by archiving change add-guard-for-cypher. Update Purpose after archive.

## Requirements
### Requirement: Cypher query parsing and detection
The system SHALL support the identification of `cypher` and `neo4j` dialects and delegate the verification to a Cypher-specific safety validation handler.

#### Scenario: Identify cypher dialect
- **WHEN** a client requests verification with dialect as "cypher" or "neo4j"
- **THEN** the system processes the query using Cypher-specific safety validation rules

### Requirement: Enforce read-only Cypher operations
The system SHALL reject any Cypher query containing mutating operations or clauses, specifically `CREATE`, `DELETE`, `DETACH`, `SET`, `REMOVE`, and `MERGE`.

#### Scenario: Mutation query with CREATE is rejected
- **WHEN** a client sends the query "MATCH (n) CREATE (m:Node) RETURN n"
- **THEN** the query is marked as not allowed with an error indicating mutating statements are prohibited

#### Scenario: Mutation query with DETACH DELETE is rejected
- **WHEN** a client sends the query "MATCH (u:User {id: 123}) DETACH DELETE u"
- **THEN** the query is marked as not allowed with an error indicating mutating statements are prohibited

### Requirement: Enforce node label and relationship type whitelist
The system SHALL verify all node labels and relationship types queried in the query patterns against the configured list of allowed tables. Node labels and relationship types will map directly to the configured table names.

#### Scenario: Query referencing unauthorized label is rejected
- **WHEN** a client sends the query "MATCH (a:Admin) RETURN a.name" with only "User" listed as an allowed table in the configuration
- **THEN** the query is marked as not allowed with an error indicating label "Admin" is not permitted

### Requirement: Enforce property whitelist
The system SHALL verify all accessed properties on nodes or relationships against the configured list of allowed columns. Properties will map directly to the configured column names.

#### Scenario: Query referencing unauthorized property is rejected
- **WHEN** a client sends the query "MATCH (u:User) RETURN u.password" with "password" not in the allowed columns for "User" in the configuration
- **THEN** the query is marked as not allowed with an error indicating property "password" is not permitted

### Requirement: Property value restriction enforcement
The system SHALL enforce property value restrictions. If a property is restricted to a specific value in the configuration, the system must ensure the query either explicitly filters on this restricted value or rejects the query if it does not match.

#### Scenario: Query complies with restriction
- **WHEN** a client sends the query "MATCH (u:User) WHERE u.accountId = 123 RETURN u.name" and the configuration requires "accountId" = 123
- **THEN** the query is marked as allowed with no errors

#### Scenario: Query violates restriction
- **WHEN** a client sends the query "MATCH (u:User) WHERE u.accountId = 456 RETURN u.name" and the configuration requires "accountId" = 123
- **THEN** the query is marked as not allowed with an error indicating a restriction violation
