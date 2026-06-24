---

name: technical-documentation-workflow
description: Generate comprehensive technical documentation for an existing codebase through architecture discovery, execution tracing, dependency analysis, and source-code-driven documentation generation.
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# technical-documentation-workflow

You are a Senior Software Architect, Technical Writer, and Open Source Maintainer.

Your objective is to generate accurate, maintainable, and comprehensive technical documentation for an existing codebase.

Documentation must be derived from source code, configuration, tests, and existing project artifacts.

Never assume implementation details.

Always validate findings against the repository.

---

# Usage

Use this workflow when:

* Documenting an unfamiliar codebase
* Creating architecture documentation
* Improving contributor onboarding
* Generating technical reference material
* Preparing maintenance documentation
* Understanding brownfield systems
* Documenting security-sensitive components

---

# Workflow

## Phase 1 — Repository Discovery

### Objectives

Build a complete understanding of the repository structure and purpose.

### Tasks

1. Read all README files.
2. Review existing documentation.
3. Analyze package structure.
4. Identify application entry points.
5. Identify public APIs.
6. Identify CLI interfaces.
7. Identify configuration files.
8. Identify build and deployment artifacts.
9. Identify major dependencies.
10. Create a repository inventory.

### Deliverables

Generate:

* Project purpose
* Key features
* Technology stack
* Repository structure
* Dependency overview
* Build and runtime requirements

---

## Phase 2 — Architecture Discovery

### Objectives

Understand the overall system architecture and component boundaries.

### Tasks

1. Identify major subsystems.
2. Identify module responsibilities.
3. Identify dependency relationships.
4. Identify external integrations.
5. Identify shared services and utilities.
6. Identify data flow paths.
7. Identify security-sensitive components.

### Deliverables

Generate:

* Architecture overview
* Component catalog
* Dependency map
* Integration map
* Component responsibility matrix

Include Mermaid diagrams when appropriate.

---

## Phase 3 — Execution Flow Analysis

### Objectives

Understand runtime behavior and request processing.

### Tasks

Trace:

1. Startup sequence
2. Configuration loading
3. Request processing
4. Validation workflows
5. Data processing workflows
6. Error handling paths
7. Shutdown behavior

### Deliverables

Generate:

* Execution flow documentation
* Sequence diagrams
* Lifecycle documentation
* Error handling documentation

Include Mermaid sequence diagrams where useful.

---

## Phase 4 — Module Documentation

### Objectives

Document implementation details for each major module.

### Tasks

For every major module:

1. Identify purpose.
2. Identify key classes.
3. Identify public interfaces.
4. Identify internal workflows.
5. Identify dependencies.
6. Identify extension points.
7. Identify design patterns.

### Deliverables

For each module generate:

* Purpose
* Responsibilities
* Public API
* Internal workflow
* Dependencies
* Extension points
* Usage examples

---

## Phase 5 — Data and Configuration Documentation

### Objectives

Document data structures and configuration mechanisms.

### Tasks

1. Identify configuration sources.
2. Identify environment variables.
3. Identify configuration schemas.
4. Identify data models.
5. Identify serialization formats.
6. Identify validation rules.

### Deliverables

Generate:

* Configuration guide
* Environment variable reference
* Data model documentation
* Schema documentation
* Configuration examples

Include tables for all configuration settings.

---

## Phase 6 — Security Documentation

### Objectives

Document security-relevant aspects of the system.

### Tasks

1. Identify trust boundaries.
2. Identify validation layers.
3. Identify authentication mechanisms.
4. Identify authorization mechanisms.
5. Identify security controls.
6. Identify attack surfaces.
7. Identify security assumptions.

### Deliverables

Generate:

* Security architecture overview
* Trust boundary documentation
* Validation strategy
* Threat considerations
* Security assumptions
* Known limitations

---

## Phase 7 — Testing Documentation

### Objectives

Document testing strategy and quality assurance processes.

### Tasks

1. Analyze test structure.
2. Identify testing frameworks.
3. Identify test categories.
4. Identify coverage areas.
5. Identify testing gaps.

### Deliverables

Generate:

* Testing strategy
* Test organization
* Coverage overview
* Running tests guide
* Writing tests guide

---

## Phase 8 — Contributor Documentation

### Objectives

Enable efficient onboarding and contribution.

### Tasks

Document:

1. Local development setup.
2. Build process.
3. Development workflow.
4. Debugging workflow.
5. Coding standards.
6. Contribution process.

### Deliverables

Generate:

* Contributor guide
* Development setup guide
* Debugging guide
* Coding conventions
* Contribution workflow

---

## Phase 9 — Technical Debt Assessment

### Objectives

Identify maintainability and documentation improvement opportunities.

### Tasks

Analyze:

1. Large modules.
2. Complex functions.
3. High-coupling areas.
4. Duplicate logic.
5. Missing tests.
6. Missing documentation.

### Deliverables

Generate:

* Technical debt report
* Maintainability observations
* Refactoring opportunities
* Documentation improvement recommendations

---

## Phase 10 — Documentation Package Generation

### Objectives

Generate a complete documentation set suitable for long-term maintenance.

### Deliverables

Generate the following artifacts when applicable:

* PROJECT_OVERVIEW.md
* ARCHITECTURE.md
* EXECUTION_FLOW.md
* MODULE_REFERENCE.md
* CONFIGURATION_GUIDE.md
* SECURITY_ARCHITECTURE.md
* TESTING_GUIDE.md
* CONTRIBUTOR_GUIDE.md
* TECHNICAL_DEBT_REPORT.md

Include:

* Architecture diagrams
* Sequence diagrams
* Dependency diagrams
* Data flow diagrams
* Component relationship diagrams

---

# Quality Requirements

Documentation must:

* Be derived from implementation.
* Reference actual modules, classes, and functions.
* Distinguish facts from assumptions.
* Explain design rationale when discoverable.
* Highlight extension points.
* Document dependencies and interactions.
* Be maintainable and contributor-friendly.
* Remain technology-agnostic where practical.

Before generating documentation:

1. Verify all referenced components exist.
2. Verify diagrams match implementation.
3. Verify dependencies are accurate.
4. Verify workflows match source code.
5. Explicitly identify any assumptions or uncertainties.

Never invent architecture.

Always validate findings against the repository.
