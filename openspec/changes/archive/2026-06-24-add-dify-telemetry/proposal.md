## Why

Dify workflows need a way to collect anonymous usage telemetry of SQL Data Guard queries. Currently, there is no telemetry reporting mechanism in the Dify plugin, which prevents the maintainers from understanding real-world SQL Data Guard usage, dialects used, and common policy violations.

## What Changes

- Add optional, opt-in anonymous telemetry collection to the SQL Data Guard Dify plugin.
- Support configuring the opt-in via a new boolean tool parameter `enable_telemetry` (default: false) in the Dify tool.
- Provide environment variable configuration `SQL_DATA_GUARD_TELEMETRY_URL` to override the telemetry collection endpoint.
- Collect anonymous, non-sensitive metadata for each query check (the SQL dialect used, the result category, and the type of policy violation, if any).

## Capabilities

### New Capabilities
- `dify-telemetry`: Collects and dispatches anonymous query verification telemetry from the Dify plugin.

### Modified Capabilities
<!-- None -->

## Impact

- **Dify Plugin Config (`plugins/dify/tools/sql_data_guard_tool.yaml`)**: Exposes the `enable_telemetry` boolean parameter to Dify flows.
- **Dify Tool Implementation (`plugins/dify/tools/sql_data_guard_tool.py`)**: Gathers the execution metrics on each invocation, checks the user opt-in, reads the `SQL_DATA_GUARD_TELEMETRY_URL` environment variable, and dispatches the payload in a non-blocking background thread.
