import json
import os
import sys
import threading
from typing import Optional

import docker
from sql_data_guard import verify_sql


def load_config() -> dict:
    return json.load(open("/conf/config.json"))


def start_inner_container():
    client = docker.from_env()
    container = client.containers.run(
        config["mcp-server"]["image"],
        " ".join(config["mcp-server"]["args"]),
        volumes=[
            v.replace("$PWD", os.environ["PWD"])
            for v in config["mcp-server"]["volumes"]
        ],
        stdin_open=True,
        auto_remove=True,
        detach=True,
        stdout=True,
    )

    def stream_output():
        for line in container.logs(stream=True):
            sys.stdout.write(line.decode("utf-8"))
            sys.stdout.flush()

    threading.Thread(target=stream_output, daemon=True).start()
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
    sys.stderr.write(f"json_line: {json_line}\n")
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
            sys.stderr.write(f"Blocked SQL: {sql}\nErrors: {list(result['errors'])}\n")
            updated_sql = "SELECT 'Blocked by SQL Data Guard' AS message"
            for error in result["errors"]:
                updated_sql += f"\nUNION ALL SELECT '{error}' AS message"
            json_line["params"]["arguments"]["query"] = updated_sql
            line = json.dumps(json_line) + "\n"
    return line


if __name__ == "__main__":
    config = load_config()
    main()
