# dify-telemetry Specification

## Purpose
TBD - created by archiving change add-dify-telemetry. Update Purpose after archive.
## Requirements
### Requirement: Configuration of telemetry
The system SHALL support configuring anonymous telemetry collection through a boolean tool parameter `enable_telemetry` and an environment variable `SQL_DATA_GUARD_TELEMETRY_URL`.

#### Scenario: Telemetry parameter enabled
- **WHEN** the tool parameter `enable_telemetry` is true and a query is verified
- **THEN** the system dispatches anonymous usage telemetry to the configured telemetry URL

#### Scenario: Telemetry parameter disabled
- **WHEN** the tool parameter `enable_telemetry` is false or not provided
- **THEN** the system does not dispatch any telemetry

#### Scenario: Custom telemetry URL
- **WHEN** `SQL_DATA_GUARD_TELEMETRY_URL` is set in the environment and telemetry is enabled
- **THEN** the system dispatches metrics to that specific URL

#### Scenario: Default telemetry URL fallback
- **WHEN** `SQL_DATA_GUARD_TELEMETRY_URL` is not set and telemetry is enabled
- **THEN** the system dispatches metrics to the fallback URL `https://telemetry.sql-data-guard.org/metrics`

### Requirement: Telemetry payload classification
The system SHALL map the SQL verification output to correct metrics without collecting any sensitive query or user data.

#### Scenario: Allowed query payload
- **WHEN** a query is allowed by verification
- **THEN** the telemetry payload contains `result` as "allowed", `dialect` matching the used dialect, and no violation type

#### Scenario: Blocked query with column violation payload
- **WHEN** a query is blocked with errors containing column restrictions
- **THEN** the telemetry payload contains `result` as "blocked" and `violation_type` as "column"

#### Scenario: Fixed query with table violation payload
- **WHEN** a query is non-compliant with errors containing table restrictions and is successfully rewritten
- **THEN** the telemetry payload contains `result` as "fixed" and `violation_type` as "table"

### Requirement: Asynchronous and silent telemetry dispatch
The system SHALL dispatch telemetry requests asynchronously in a daemon background thread and catch any network or timeout errors silently.

#### Scenario: Asynchronous telemetry dispatch
- **WHEN** telemetry is triggered
- **THEN** the tool returns immediately to the Dify workflow without waiting for the network call to finish

#### Scenario: Silently handle network failure
- **WHEN** the telemetry POST request fails (due to timeout, offline host, or bad certificate)
- **THEN** the error is captured silently and does not crash the Dify tool execution

