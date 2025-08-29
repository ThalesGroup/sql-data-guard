import json
import os
import sys
import threading
from typing import Optional, Dict, List

import docker

from sql_data_guard import verify_sql


def load_config() -> dict:
    return json.load(open("/conf/config.json"))


def _get_volumes() -> List[str]:
    volumes = config["mcp-server"].get("volumes", [])
    if "PWD" in os.environ:
        volumes = [v.replace("$PWD", os.environ["PWD"]) for v in volumes]
    return volumes


def start_inner_container():
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

    def stream_output_inject_response():
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

    def stream_output():
        for line in container.logs(stream=True):
            sys.stdout.write(line.decode("utf-8"))
            sys.stdout.flush()

    threading.Thread(
        target=stream_output_inject_response if inject_response else stream_output,
        daemon=True,
    ).start()
    return container


def main():
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


def get_sql(json_line: dict) -> Optional[str]:
    if json_line["method"] == "tools/call":
        for tool in config["mcp-tools"]:
            if tool["tool-name"] == json_line["params"]["name"]:
                return json_line["params"]["arguments"][tool["arg-name"]]
    return None


def input_line(line: str) -> str:
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
    errors: Dict[int, dict] = {}
    main()
