## Context

The SQL Data Guard Dify plugin validates user-generated SQL queries before they run on a database. To continuously improve SQL Data Guard's coverage and identify common SQL injection/policy-bypassing patterns, the plugin will collect lightweight, anonymous metrics on each check, provided the user explicitly opts in.

## Goals / Non-Goals

**Goals:**
- Provide optional, opt-in anonymous telemetry collection configured via a new `enable_telemetry` parameter (defaulting to `false`).
- Read the destination endpoint from the `SQL_DATA_GUARD_TELEMETRY_URL` environment variable, defaulting to a secure fallback: `https://telemetry.sql-data-guard.org/metrics`.
- Send non-sensitive query metadata (SQL dialect, allowed/blocked/fixed result, and column/table/other violation classification) to the endpoint.
- Ensure telemetry calls are fully non-blocking and execute in an asynchronous background thread.
- Handle any network or endpoint failures silently without affecting the core query verification.

**Non-Goals:**
- Collect any personal or sensitive information (no raw SQL queries, no schema details, no credentials, no table/column names).
- Implement persistent client-side queuing or retrying of telemetry payloads.
- Introduce heavy external Python library dependencies (like `requests` or `httpx`).

## Decisions

### Decision 1: Configuration of Telemetry Endpoint via Environment Variable
- **Rationale**: We will read the telemetry URL from the `SQL_DATA_GUARD_TELEMETRY_URL` environment variable. This allows system administrators or operators deploying the Dify plugin to change or route the telemetry stream to their own internal telemetry aggregation or logging pipelines without modifying code.
- **Alternatives considered**: Hardcoding the URL or putting it inside `manifest.yaml`. This was rejected as it lacks flexibility and prevents private deployments from redirection.

### Decision 2: Tool Parameter Opt-in
- **Rationale**: Adding `enable_telemetry` as a boolean parameter in `sql_data_guard_tool.yaml`. This allows end-users of Dify workflows to toggle telemetry per-tool or per-workflow, maximizing privacy consent.
- **Alternatives considered**: Opt-out by default. Rejected because security and privacy-first design dictates explicit opt-in (`default: false`).

### Decision 3: Asynchronous Threaded Dispatch
- **Rationale**: Telemetry requests must not introduce latency into the core Dify workflow engine. We will use a standard daemon `threading.Thread` to send the HTTP payload asynchronously.
- **Alternatives considered**: Synchronous requests. Rejected because waiting for a network handshake would introduce ~50-500ms of latency per query validation.

### Decision 4: Using Python Standard Library (`urllib.request`)
- **Rationale**: To keep the plugin package lightweight and extremely portable, we will use Python's built-in `urllib` module. This guarantees it works in any restricted sandbox without needing Rust/compilation tools (which might fail, as seen with some custom Python wheels).

## Risks / Trade-offs

- **[Risk] Telemetry server is offline or slow** ➔ **Mitigation**: We set a short connection timeout (`2.0` seconds) and run the thread as a `daemon` so it does not block application exit.
- **[Risk] Failure in telemetry dispatches causes tool crash** ➔ **Mitigation**: The entire background thread execution body is wrapped in a `try...except Exception` block, silently suppressing any errors.
