"""
SQL Data Guard MCP wrapper.

A stdio proxy between an MCP client and an MCP server that runs in a Docker container. It reads the
client's JSON-RPC messages, verifies the SQL of the tool calls that run SQL, and forwards them to the
server (rewritten when the query can be fixed). Everything else that would let a caller run SQL
unchecked is refused.

Configuration (/conf/config.json, or the file named by $SDG_CONFIG):

    {
      "mcp-server": {"image": "mcp/sqlite", "args": ["--db-path", "/data/x.db"], "volumes": ["$PWD:/data"]},
      "mcp-tools": [{"tool-name": "read_query", "arg-name": "query"}],
      "passthrough-tools": ["list_tables"],
      "sql-data-guard": {"dialect": "sqlite", "inject-response": true, "tables": [...]}
    }

* `mcp-tools`: the tools that run SQL, and the argument that holds it. Their SQL is verified.
* `passthrough-tools`: tools that are forwarded untouched. Any other `tools/call` is refused, so a
  second SQL tool (for example `write_query`) cannot be used to get around the guard.
* `sql-data-guard`: the policy (see docs/manual.md) plus `dialect` and `inject-response`.

The wrapper needs access to the Docker socket to start the MCP server, which is as powerful as root on
the host: run it only where that is acceptable.
"""

import json
import os
import sys
import threading
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from sql_data_guard import verify_sql

_DEFAULT_CONFIG_PATH = "/conf/config.json"

# JSON-RPC error codes
_PARSE_ERROR = -32700
_INVALID_REQUEST = -32600
_INVALID_PARAMS = -32602


def load_config(path: Optional[str] = None) -> dict:
    path = path or os.environ.get("SDG_CONFIG", _DEFAULT_CONFIG_PATH)
    with open(path) as f:
        return json.load(f)


def validate_config(config: Any) -> None:
    """Raises ValueError with a clear message if the wrapper configuration is unusable."""
    if not isinstance(config, dict):
        raise ValueError("The configuration must be a JSON object.")
    server = config.get("mcp-server")
    if not isinstance(server, dict) or not isinstance(server.get("image"), str) or not server["image"]:
        raise ValueError("'mcp-server' must be an object with an 'image'.")
    tools = config.get("mcp-tools")
    if not isinstance(tools, list) or not tools:
        raise ValueError("'mcp-tools' must be a non-empty list of {'tool-name', 'arg-name'}.")
    names = set()
    for tool in tools:
        if (
            not isinstance(tool, dict)
            or not isinstance(tool.get("tool-name"), str)
            or not isinstance(tool.get("arg-name"), str)
            or not tool["tool-name"]
            or not tool["arg-name"]
        ):
            raise ValueError("Each entry of 'mcp-tools' needs a 'tool-name' and an 'arg-name'.")
        if tool["tool-name"] in names:
            raise ValueError(f"Tool '{tool['tool-name']}' is listed more than once in 'mcp-tools'.")
        names.add(tool["tool-name"])
    passthrough = config.get("passthrough-tools", [])
    if not isinstance(passthrough, list) or not all(isinstance(t, str) for t in passthrough):
        raise ValueError("'passthrough-tools' must be a list of tool names.")
    overlap = names & set(passthrough)
    if overlap:
        raise ValueError(f"Tools cannot be both guarded and passed through: {sorted(overlap)}.")
    guard = config.get("sql-data-guard")
    if not isinstance(guard, dict) or "tables" not in guard:
        raise ValueError("'sql-data-guard' must be an object with the policy ('tables').")
    if "dialect" in guard and guard["dialect"] is not None and not isinstance(guard["dialect"], str):
        raise ValueError("'dialect' must be a string.")


def _no_duplicate_keys(pairs: List[Tuple[str, Any]]) -> dict:
    result: dict = {}
    for key, value in pairs:
        if key in result:
            # parsers disagree on which duplicate wins: refuse instead of guessing
            raise ValueError(f"duplicate key {key!r}")
        result[key] = value
    return result


def parse_message(text: str) -> Any:
    """Parses a JSON-RPC line. Raises ValueError for invalid JSON or duplicate object keys."""
    return json.loads(text, object_pairs_hook=_no_duplicate_keys)


def iter_lines(chunks: Iterable[bytes]) -> Iterator[bytes]:
    """Splits a stream of byte chunks (which may cut or join lines) into lines."""
    buffer = b""
    for chunk in chunks:
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line
    if buffer:
        yield buffer


def _error_reply(request_id: Any, code: int, message: str) -> str:
    return json.dumps(
        {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}
    )


def _blocked_reply(request_id: Any, text: str) -> str:
    """A tool result telling the client that the call was refused (nothing reaches the server)."""
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"content": [{"type": "text", "text": text}], "isError": True},
        }
    )


class SqlGuardProxy:
    """The decisions of the wrapper, independent of Docker and of the standard streams."""

    def __init__(self, config: dict):
        validate_config(config)
        self.guard_config: dict = config["sql-data-guard"]
        self.dialect: Optional[str] = self.guard_config.get("dialect")
        self.inject_response: bool = bool(self.guard_config.get("inject-response", False))
        self.sql_arguments: Dict[str, str] = {t["tool-name"]: t["arg-name"] for t in config["mcp-tools"]}
        self.passthrough = set(config.get("passthrough-tools", []))
        # responses to annotate (inject-response), by JSON-RPC id
        self._pending: Dict[Any, dict] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ client -> server
    def handle_request(self, line: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Decides what to do with one line from the client.

        Returns (forward, reply): `forward` is sent to the MCP server, `reply` is sent straight back
        to the client. Either can be None.
        """
        if not line.strip():
            return None, None
        try:
            message = parse_message(line)
        except ValueError as e:
            self._log(f"Dropped an unreadable message: {e}")
            return None, _error_reply(None, _PARSE_ERROR, "Parse error")
        if isinstance(message, list):
            return None, _error_reply(None, _INVALID_REQUEST, "Batch requests are not supported")
        if not isinstance(message, dict):
            return None, _error_reply(None, _INVALID_REQUEST, "Invalid request")
        if message.get("method") != "tools/call":
            # initialize, tools/list, notifications, and the client's answers to server requests
            return line if line.endswith("\n") else line + "\n", None
        return self._tools_call(message)

    def _tools_call(self, message: dict) -> Tuple[Optional[str], Optional[str]]:
        request_id = message.get("id")
        has_id = "id" in message
        params = message.get("params")
        if not isinstance(params, dict) or not isinstance(params.get("name"), str):
            return None, _error_reply(request_id, _INVALID_PARAMS, "Invalid params") if has_id else None
        name: str = params["name"]
        if name in self.passthrough:
            return json.dumps(message) + "\n", None
        arg_name = self.sql_arguments.get(name)
        if arg_name is None:
            self._log(f"Refused the call of tool '{name}': it is not configured")
            return None, self._refuse(has_id, request_id, f"Tool '{name}' is not allowed by SQL Data Guard")

        arguments = params.get("arguments")
        if not isinstance(arguments, dict) or not isinstance(arguments.get(arg_name), str):
            return None, self._refuse(
                has_id, request_id, f"Blocked by SQL Data Guard: argument '{arg_name}' must be an SQL string"
            )
        sql: str = arguments[arg_name]

        result = verify_sql(sql, self.guard_config, self.dialect)
        errors = sorted(result["errors"])
        if result["allowed"]:
            return json.dumps(message) + "\n", None

        self._log(f"ID: {request_id} Blocked SQL: {sql!r} Errors: {errors}")
        fixed = result["fixed"]
        # the rewritten query is trusted only if it passes the same verification
        if fixed and verify_sql(fixed, self.guard_config, self.dialect)["allowed"]:
            self._log(f"Fixed SQL: {fixed!r}")
            arguments[arg_name] = fixed
            if self.inject_response and has_id:
                with self._lock:
                    self._pending[request_id] = {
                        "errors": errors,
                        "fixed": fixed,
                        "risk": result["risk"],
                    }
            return json.dumps(message) + "\n", None
        # nothing is sent to the server: no SQL is built from the errors, which contain parts of the query
        return None, self._refuse(
            has_id, request_id, "Blocked by SQL Data Guard: " + "; ".join(errors or ["the query is not allowed"])
        )

    @staticmethod
    def _refuse(has_id: bool, request_id: Any, text: str) -> Optional[str]:
        return _blocked_reply(request_id, text) if has_id else None

    # ------------------------------------------------------------------ server -> client
    def handle_response(self, text: str) -> Optional[str]:
        """
        Processes one line from the server. Returns the line for the client, or None if it is not a
        JSON message (server logs), which must not reach the client's JSON-RPC stream.
        """
        text = text.strip()
        if not text:
            return None
        try:
            message = json.loads(text)
        except ValueError:
            return None
        if not isinstance(message, dict):
            return text
        if self.inject_response and "id" in message:
            with self._lock:
                info = self._pending.pop(message["id"], None) if _hashable(message["id"]) else None
            result = message.get("result")
            if info is not None and isinstance(result, dict) and isinstance(result.get("content"), list):
                result["content"].insert(
                    0, {"type": "text", "text": f"[SQL Data Guard: {json.dumps(info)}]"}
                )
                return json.dumps(message)
        return text

    @staticmethod
    def _log(text: str) -> None:
        sys.stderr.write(text + "\n")
        sys.stderr.flush()


def _hashable(value: Any) -> bool:
    try:
        hash(value)
    except TypeError:
        return False
    return True


# ---------------------------------------------------------------------- runtime
_stdout_lock = threading.Lock()


def _write_stdout(line: str) -> None:
    with _stdout_lock:
        sys.stdout.write(line if line.endswith("\n") else line + "\n")
        sys.stdout.flush()


def _get_volumes(config: dict) -> List[str]:
    volumes = config["mcp-server"].get("volumes", [])
    if "PWD" in os.environ:
        volumes = [v.replace("$PWD", os.environ["PWD"]) for v in volumes]
    return volumes


def start_inner_container(config: dict, proxy: SqlGuardProxy):
    import docker  # only needed at run time, so the logic above is importable (and testable) without it

    server = config["mcp-server"]
    client = docker.from_env()
    container = client.containers.run(
        server["image"],
        " ".join(server["args"]) if "args" in server else None,
        volumes=_get_volumes(config),
        network_mode=server.get("network-mode"),
        stdin_open=True,
        auto_remove=True,
        detach=True,
        stdout=True,
    )

    def pump_output() -> None:
        for raw in iter_lines(container.logs(stream=True)):
            text = raw.decode("utf-8", errors="replace")
            out = proxy.handle_response(text)
            if out is None:
                if text.strip():
                    sys.stderr.write(f"[mcp-server] {text}\n")
                    sys.stderr.flush()
            else:
                _write_stdout(out)

    threading.Thread(target=pump_output, daemon=True).start()
    return container


def main() -> int:
    try:
        config = load_config()
        proxy = SqlGuardProxy(config)
    except (OSError, ValueError) as e:
        sys.stderr.write(f"Invalid SQL Data Guard configuration: {e}\n")
        return 2
    container = start_inner_container(config, proxy)
    try:
        sock = container.attach_socket(params={"stdin": True, "stream": True})
        # noinspection PyProtectedMember
        sock._sock.setblocking(True)
        for line in sys.stdin:
            forward, reply = proxy.handle_request(line)
            if reply is not None:
                _write_stdout(reply)
            if forward is not None:
                # noinspection PyProtectedMember
                sock._sock.sendall(forward.encode("utf-8"))
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        container.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
