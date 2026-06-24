## ADDED Requirements

### Requirement: README must include visual architecture diagram
The system's main documentation (README.md) SHALL contain a polished ASCII diagram representing the 4 integration channels of sql-data-guard.

#### Scenario: Visual architecture rendering
- **WHEN** a user reads the README.md
- **THEN** they see an ASCII diagram outlining Python SDK, REST API, MCP Wrapper, and Dify Plugin as a security gateway between the LLM and the Database.

### Requirement: README must explain the Vibe Coding workflow
The documentation SHALL feature a specific section called "Vibe Coding & Agentic Era Ready" explaining how opencode, OpenSpec, and GitHub MCP servers are used to maintain and evolve the repository.

#### Scenario: Vibe Coding section layout
- **WHEN** a developer views the README.md
- **THEN** they see the vibe coding feedback loop diagram and details on opencode and OpenSpec.

### Requirement: README must list the repository organization
The documentation SHALL provide a clear repository anatomy mapping key files and directories to their purpose.

#### Scenario: File structure clarity
- **WHEN** a developer is navigating the repository
- **THEN** they see a clear folder hierarchy representation of src/, plugins/, examples/, openspec/, docs/, tests/, etc.

### Requirement: README must use Podman as the local container runtime
All local run and deploy commands within the README.md SHALL use Podman commands instead of Docker.

#### Scenario: Local run verification
- **WHEN** a developer copies commands to run the REST API or MCP wrapper locally
- **THEN** they use podman run instead of docker run.
