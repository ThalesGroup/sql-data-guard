# MCP wrapper

The wrapper sits between an MCP client (an LLM application) and an MCP server that talks to a database. It verifies the SQL of the tool calls with sql-data-guard before the server sees them, so an LLM can only run queries that comply with your policy.

```text
MCP client  <-- stdio -->  sql-data-guard wrapper  <-- Docker -->  MCP server (for example mcp/sqlite)
```

The wrapper starts the MCP server itself, as a Docker container, so it needs the Docker socket:

```bash
docker run --rm -i \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $PWD/config.json:/conf/config.json \
  -e PWD=$PWD \
  ghcr.io/thalesgroup/sql-data-guard-mcp:latest
```

> **Security note:** access to the Docker socket is as powerful as root on the host. Run the wrapper only where that is acceptable, and never give an untrusted party control of its configuration.

## Configuration

`/conf/config.json` (or the file named by `$SDG_CONFIG`):

```json
{
  "mcp-server": {
    "image": "mcp/sqlite",
    "args": ["--db-path", "/data/countries.db"],
    "volumes": ["$PWD:/data"],
    "network-mode": "host"
  },
  "mcp-tools": [{"tool-name": "read_query", "arg-name": "query"}],
  "passthrough-tools": ["list_tables", "describe_table"],
  "sql-data-guard": {
    "dialect": "sqlite",
    "inject-response": true,
    "tables": [
      {
        "table_name": "countries",
        "columns": ["name", "region"],
        "restrictions": [{"column": "region", "value": "Asia"}]
      }
    ]
  }
}
```

| Key | Description |
|-----|-------------|
| `mcp-server` | The MCP server container: `image` (required), `args`, `volumes` (`$PWD` is replaced by the `PWD` environment variable) and `network-mode`. |
| `mcp-tools` | The tools that run SQL, and the argument that holds the query (`arg-name`). Their SQL is verified. |
| `passthrough-tools` | Tools that are forwarded untouched, such as `list_tables`. Optional. |
| `sql-data-guard` | The policy, as described in the [manual](manual.md), plus `dialect` and `inject-response`. |

**Every other tool is refused.** A tool that is neither in `mcp-tools` nor in `passthrough-tools` is not forwarded, because a second SQL tool (the sqlite server also has `write_query` and `create_table`) would let the LLM run any query and skip the guard. List only the tools you want available. Note that metadata tools such as `describe_table` reveal the columns of the whole table, including the ones your policy does not allow.

## What happens to a query

| Verification result | The wrapper |
|---------------------|-------------|
| Allowed | Forwards the call. |
| Not allowed, but can be repaired | Forwards the call with the repaired query, in the same argument. With `inject-response`, the response also carries what was changed (errors, the repaired query, risk). |
| Not allowed and cannot be repaired | Answers the client itself with a tool error (`isError: true`) that lists the problems. Nothing reaches the server. |

The repaired query is verified again before it is sent. A call that has no SQL string in `arg-name`, a malformed message, a batch, or a message with duplicate JSON keys (parsers disagree on which one wins) is refused too. The wrapper forwards exactly the message it verified.

Lines of the server's output that are not JSON-RPC messages (logs) go to the wrapper's standard error, so the client's protocol stream stays clean.

## Example

`examples/mcp-wrapper-sqlite` runs an LLM agent against a SQLite MCP server through the wrapper.
