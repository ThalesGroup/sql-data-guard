## Context

The current `README.md` is dry, lacklustre, and out-of-date. It only showcases raw Python/Docker setups and ignores modern developer trends (e.g., emojis, "vibe coding", agentic workflows) and crucial project capabilities (MCP wrappers, Dify integration). We need to design a highly engaging, fancy, emoji-rich README that clearly presents `sql-data-guard` as the premier agent-ready SQL security tool.

## Goals / Non-Goals

**Goals:**
- Replace the legacy `README.md` with a vibrant, beautifully formatted document.
- Highlight the 4 ways to consume `sql-data-guard` (Python Library, REST API, MCP Server Wrapper, Dify Plugin).
- Switch all docker commands to podman.
- Showcase our Agentic/Vibe Coding workflow with ASCII art.
- Document repository structure clearly.

**Non-Goals:**
- Modifying any underlying Python, REST API, or MCP wrapper codebase.
- Re-architecting existing code paths or modifying code logic.

## Decisions

### Decision 1: Use Podman instead of Docker for local run examples
- **Rationale**: Podman is rootless, safer, and fully compatible with Docker images (using `ghcr.io/thalesgroup/sql-data-guard`). It aligns with contemporary secure container standards.
- **Alternatives considered**: Keeping Docker (rejected because Podman is modern and requested by the user), supporting both (rejected to avoid cluttering commands, but we will mention compatibility).

### Decision 2: Feature the 4 Integration Channels prominently
- **Rationale**: SQL Data Guard can be consumed in multiple ways. Listing Python SDK, REST API, MCP Wrapper, and Dify Plugin as clear pillars makes it easy for developers to pick their integration point.
- **Alternatives considered**: Putting details in sub-pages (rejected, as keeping quickstarts in the main README is standard for developer onboarding).

### Decision 3: Add "Vibe Coding" & "Agentic Era Ready" sections
- **Rationale**: Shows that this project is modern, automated, and built natively with AI agents (using OpenSpec/opencode).
- **Alternatives considered**: Keeping development details only in `CONTRIBUTING.md` (rejected, as the vibe of a repo starts on the home page).

## Risks / Trade-offs

- **Risk**: Some developers might not have Podman installed.
- **Mitigation**: Add a brief note that Podman commands can be used with `docker` interchangeably if they have Docker installed.
