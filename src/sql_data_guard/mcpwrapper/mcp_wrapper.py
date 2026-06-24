"""
SQL Data Guard MCP wrapper.

This module acts as an interception layer between an MCP client and an inner
MCP server container (like PostgreSQL or SQLite). It monitors incoming tool calls,
verifies/rewrites SQL query arguments against policies, and optionally injects error annotations.
"""

import json
import os
import sys
import threading
from pathlib import Path
from typing import Any

import docker

from sql_data_guard import verify_sql


def load_config() -> dict[str, Any]:
    """
    Loads the system configuration JSON file.

    Returns:
        dict[str, Any]: The parsed configuration dictionary.
    """
    return json.load(Path("/conf/config.json").open())


def _get_volumes() -> list[str]:
    """
    Computes list of mount volumes with resolved environment paths (e.g. $PWD).

    Returns:
        list[str]: Docker volumes list.
    """
    volumes = config["mcp-server"].get("volumes", [])
    if "PWD" in os.environ:
        volumes = [v.replace("$PWD", os.environ["PWD"]) for v in volumes]
    return volumes


def start_inner_container() -> Any:
    """
    Spawns and configures the inner MCP server container.

    Returns:
        Any: The running Docker Container object.
    """
    client = docker.from_env()
    container = client.containers.run(
        config["mcp-server"]["image"],
        (
            " ".join(config["mcp-server"]["args"])
            if "args" in config["mcp-server"]
            else None
        ),
        volumes=_get_volumes(),
        network_mode=config["mcp-server"].get("network-mode"),
        stdin_open=True,
        auto_remove=True,
        detach=True,
        stdout=True,
    )

    def stream_output_inject_response() -> None:
        """
        Streams container stdout/logs to stdout, injecting error descriptions
        into the responses if the corresponding request was blocked.
        """
        for line in container.logs(stream=True):
            line_json = json.loads(line)
            request_id = line_json["id"]
            if request_id in errors:
                if "result" in line_json and "content" in line_json["result"]:
                    line_json["result"]["content"].insert(
                        0,
                        {
                            "type": "text",
                            "text": f"[{errors[request_id]}]",
                            "isError": True,
                        },
                    )
                del errors[request_id]
            sys.stdout.write(json.dumps(line_json) + "\n")
            sys.stdout.flush()

    def stream_output() -> None:
        """
        Streams container logs to system standard output without modification.
        """
        for line in container.logs(stream=True):
            sys.stdout.write(line.decode("utf-8"))
            sys.stdout.flush()

    threading.Thread(
        target=stream_output_inject_response if inject_response else stream_output,
        daemon=True,
    ).start()
    return container


def main() -> None:
    """
    Main entry point for running the MCP wrapper interception proxy loop.
    """
    container = start_inner_container()
    try:
        socket = container.attach_socket(params={"stdin": True, "stream": True})
        # noinspection PyProtectedMember
        socket._sock.setblocking(True)
        for line in sys.stdin:
            line = input_line(line)
            # noinspection PyProtectedMember
            socket._sock.sendall(line.encode("utf-8"))
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        container.stop()


def get_sql(json_line: dict[str, Any]) -> str | None:
    """
    Extracts the SQL query string from a parsed JSON-RPC request line if it represents a monitored tool call.

    Args:
        json_line (dict[str, Any]): The parsed JSON-RPC request line dictionary.

    Returns:
        str | None: The SQL query string if found, otherwise None.
    """
    if json_line["method"] == "tools/call":
        for tool in config["mcp-tools"]:
            if tool["tool-name"] == json_line["params"]["name"]:
                return json_line["params"]["arguments"][tool["arg-name"]]
    return None


def input_line(line: str) -> str:
    """
    Processes and potentially intercepts/rewrites an incoming JSON-RPC stdin request line.

    Args:
        line (str): The raw input line from stdin.

    Returns:
        str: The processed JSON-RPC request string.
    """
    json_line = json.loads(line.encode("utf-8"))
    sql = get_sql(json_line)
    if sql:
        result = verify_sql(
            sql,
            config["sql-data-guard"],
            config["sql-data-guard"]["dialect"],
        )
        if not result["allowed"]:
            sys.stderr.write(
                f"ID: {json_line['id']} Blocked SQL: {sql}\nErrors: {list(result['errors'])}\n"
            )
            if result["fixed"]:
                sys.stderr.write(f"Fixed SQL: {result['fixed']}\n")
                updated_sql = result["fixed"]
            else:
                updated_sql = "SELECT 'Blocked by SQL Data Guard' AS message"
                if not inject_response:
                    for error in result["errors"]:
                        updated_sql += f"\nUNION ALL SELECT '{error}' AS message"
            if inject_response:
                result["errors"] = list(result["errors"])
                if result["fixed"]:
                    result["fixed"] = result["fixed"].replace("'", "''")
                errors[json_line["id"]] = result
            json_line["params"]["arguments"]["query"] = updated_sql
            line = json.dumps(json_line) + "\n"
    return line


if __name__ == "__main__":
    config = load_config()
    inject_response = config["sql-data-guard"]["inject-response"]
    errors: dict[int, dict[str, Any]] = {}
    main()
