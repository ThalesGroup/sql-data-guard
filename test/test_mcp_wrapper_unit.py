"""The MCP wrapper's decisions (no Docker needed): what is forwarded, refused, rewritten or annotated."""

import importlib.util
import json
import os

import pytest

_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "src",
    "sql_data_guard",
    "mcpwrapper",
    "mcp_wrapper.py",
)
_spec = importlib.util.spec_from_file_location("mcp_wrapper_under_test", _PATH)
wrapper = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wrapper)


def _config(**overrides):
    config = {
        "mcp-server": {"image": "mcp/sqlite"},
        "mcp-tools": [{"tool-name": "read_query", "arg-name": "query"}],
        "sql-data-guard": {
            "dialect": "sqlite",
            "inject-response": False,
            "tables": [
                {
                    "table_name": "countries",
                    "columns": ["name", "region"],
                    "restrictions": [{"column": "region", "value": "Asia"}],
                }
            ],
        },
    }
    config.update(overrides)
    return config


def _call(sql, tool="read_query", arg="query", request_id=1, **extra_args):
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": tool, "arguments": {arg: sql, **extra_args}},
        }
    )


def _proxy(**overrides):
    return wrapper.SqlGuardProxy(_config(**overrides))


ALLOWED = "SELECT name FROM countries WHERE region = 'Asia'"


# ------------------------------------------------------------------ allowed / fixed / blocked
def test_allowed_query_is_forwarded_unchanged():
    forward, reply = _proxy().handle_request(_call(ALLOWED))
    assert reply is None
    assert json.loads(forward)["params"]["arguments"]["query"] == ALLOWED
    assert forward.endswith("\n")


def test_fixable_query_is_rewritten_in_the_configured_argument():
    forward, reply = _proxy().handle_request(_call("SELECT name FROM countries"))
    assert reply is None
    sql = json.loads(forward)["params"]["arguments"]["query"]
    assert sql == "SELECT name FROM countries WHERE region = 'Asia'"


def test_rewrite_goes_to_the_configured_argument_name_not_to_query():
    # the postgres example uses arg-name "sql": the blocked text must not stay in "sql"
    proxy = _proxy(**{"mcp-tools": [{"tool-name": "query", "arg-name": "sql"}]})
    forward, _ = proxy.handle_request(_call("SELECT name FROM countries", tool="query", arg="sql"))
    arguments = json.loads(forward)["params"]["arguments"]
    assert arguments == {"sql": "SELECT name FROM countries WHERE region = 'Asia'"}


def test_unfixable_query_is_answered_by_the_wrapper_and_never_reaches_the_server():
    forward, reply = _proxy().handle_request(_call("SELECT secret FROM countries", request_id=7))
    assert forward is None
    answer = json.loads(reply)
    assert answer["id"] == 7 and answer["result"]["isError"] is True
    assert "Blocked by SQL Data Guard" in answer["result"]["content"][0]["text"]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT name FROM countries WHERE region = 'Asia'; DROP TABLE countries",
        "DELETE FROM countries",
        "SELECT name FROM other",
        "",
        "   ",
    ],
)
def test_dangerous_or_empty_sql_is_not_forwarded(sql):
    forward, reply = _proxy().handle_request(_call(sql))
    assert forward is None and json.loads(reply)["result"]["isError"] is True


def test_error_text_is_never_turned_into_sql():
    # errors contain parts of the query (identifiers, literals); they used to be pasted into a
    # "UNION ALL SELECT '<error>'" query that the server then ran
    hostile = "SELECT name FROM countries WHERE region = 'Asia' AND 'x''; DROP TABLE countries; --' = 'y' OR 1 = 1"
    forward, reply = _proxy().handle_request(_call(hostile))
    if forward is not None:  # a repaired query is allowed to reach the server, nothing built from errors is
        sql = json.loads(forward)["params"]["arguments"]["query"]
        assert "UNION ALL SELECT" not in sql and "DROP TABLE" not in sql
    else:
        answer = json.loads(reply)  # a well-formed answer to the client
        assert answer["result"]["isError"] is True


def test_fixed_query_that_does_not_verify_is_not_trusted(monkeypatch):
    calls = []

    def fake_verify(sql, config, dialect):
        calls.append(sql)
        if len(calls) == 1:
            return {"allowed": False, "errors": ["e"], "fixed": "SELECT secret FROM countries", "risk": 1}
        return {"allowed": False, "errors": ["still bad"], "fixed": None, "risk": 1}

    monkeypatch.setattr(wrapper, "verify_sql", fake_verify)
    forward, reply = _proxy().handle_request(_call("SELECT name FROM countries"))
    assert forward is None and json.loads(reply)["result"]["isError"] is True
    assert len(calls) == 2


# ------------------------------------------------------------------ which tools
def test_unlisted_tool_is_refused():
    forward, reply = _proxy().handle_request(_call("INSERT INTO countries VALUES (1)", tool="write_query"))
    assert forward is None
    assert "not allowed" in json.loads(reply)["result"]["content"][0]["text"]


def test_passthrough_tool_is_forwarded():
    proxy = _proxy(**{"passthrough-tools": ["list_tables"]})
    forward, reply = proxy.handle_request(_call("ignored", tool="list_tables"))
    assert reply is None and json.loads(forward)["params"]["name"] == "list_tables"


def test_unlisted_tool_notification_gets_no_reply():
    message = json.dumps({"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "write_query", "arguments": {}}})
    assert _proxy().handle_request(message) == (None, None)


@pytest.mark.parametrize(
    "message",
    [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 3, "result": {"roots": []}},  # the client's answer to a server request
        {"jsonrpc": "2.0", "id": 4, "error": {"code": -1, "message": "no"}},
    ],
)
def test_other_messages_are_forwarded(message):
    forward, reply = _proxy().handle_request(json.dumps(message))
    assert reply is None and json.loads(forward) == message


# ------------------------------------------------------------------ malformed input
@pytest.mark.parametrize(
    "line",
    [
        "not json",
        "{",
        "[",
        '{"method": "tools/call", "id": 1, "params": {"name": "read_query", "arguments": {"query": "a", "query": "b"}}}',
    ],
)
def test_unreadable_or_ambiguous_messages_are_dropped_with_an_error(line):
    forward, reply = _proxy().handle_request(line)
    assert forward is None
    assert json.loads(reply)["error"]["code"] == -32700


def test_duplicate_keys_cannot_hide_a_second_query():
    # one parser would use the first value, another the last
    line = (
        '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read_query","arguments":'
        '{"query":"SELECT name FROM countries WHERE region = \'Asia\'","query":"DROP TABLE countries"}}}'
    )
    forward, _ = _proxy().handle_request(line)
    assert forward is None


def test_batches_are_refused():
    forward, reply = _proxy().handle_request(json.dumps([json.loads(_call(ALLOWED))]))
    assert forward is None and json.loads(reply)["error"]["code"] == -32600


@pytest.mark.parametrize("message", ["5", '"text"', "null", "true"])
def test_messages_that_are_not_objects(message):
    forward, reply = _proxy().handle_request(message)
    assert forward is None and json.loads(reply)["error"]["code"] == -32600


@pytest.mark.parametrize(
    "params",
    [None, "x", [], {}, {"name": 5}, {"name": None}],
)
def test_tools_call_without_a_usable_name(params):
    message = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": params}
    forward, reply = _proxy().handle_request(json.dumps(message))
    assert forward is None and json.loads(reply)["error"]["code"] == -32602


@pytest.mark.parametrize(
    "arguments",
    [None, {}, [], "x", {"query": 5}, {"query": None}, {"query": ["SELECT 1"]}, {"other": ALLOWED}],
)
def test_tools_call_without_an_sql_string(arguments):
    message = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "read_query", "arguments": arguments},
    }
    forward, reply = _proxy().handle_request(json.dumps(message))
    assert forward is None and json.loads(reply)["result"]["isError"] is True


def test_blank_lines_are_ignored():
    assert _proxy().handle_request("\n") == (None, None)


# ------------------------------------------------------------------ responses
def test_inject_response_annotates_the_response_of_a_fixed_query():
    proxy = _proxy()
    proxy.inject_response = True
    forward, _ = proxy.handle_request(_call("SELECT name FROM countries", request_id="abc"))
    assert forward is not None
    response = {"jsonrpc": "2.0", "id": "abc", "result": {"content": [{"type": "text", "text": "rows"}]}}
    out = json.loads(proxy.handle_response(json.dumps(response)))
    assert out["result"]["content"][1] == {"type": "text", "text": "rows"}
    assert "SQL Data Guard" in out["result"]["content"][0]["text"]
    # only once
    again = json.loads(proxy.handle_response(json.dumps(response)))
    assert len(again["result"]["content"]) == 1


def test_responses_are_untouched_without_inject_response():
    proxy = _proxy()
    proxy.handle_request(_call("SELECT name FROM countries"))
    line = json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"content": []}})
    assert proxy.handle_response(line) == line


@pytest.mark.parametrize(
    "line",
    [
        json.dumps({"jsonrpc": "2.0", "method": "notifications/message", "params": {}}),  # no id
        json.dumps({"jsonrpc": "2.0", "id": [1], "result": {}}),  # unhashable id
        json.dumps({"jsonrpc": "2.0", "id": 1, "error": {"code": 1, "message": "x"}}),  # no result
        json.dumps([1, 2]),
    ],
)
def test_server_messages_without_an_id_or_result_do_not_crash_inject_response(line):
    proxy = _proxy()
    proxy.inject_response = True
    proxy.handle_request(_call("SELECT name FROM countries"))
    assert proxy.handle_response(line) == line


@pytest.mark.parametrize("line", ["server started", "2026-01-01 INFO listening", "{broken", ""])
def test_server_log_lines_are_not_json_messages(line):
    assert _proxy().handle_response(line) is None


def test_iter_lines_handles_chunks_that_cut_or_join_lines():
    chunks = [b'{"a": 1}\n{"b"', b": 2}\n", b"tail without newline"]
    assert list(wrapper.iter_lines(chunks)) == [b'{"a": 1}', b'{"b": 2}', b"tail without newline"]
    assert list(wrapper.iter_lines([])) == []


# ------------------------------------------------------------------ configuration
@pytest.mark.parametrize(
    "mutate",
    [
        lambda c: c.pop("mcp-server"),
        lambda c: c["mcp-server"].pop("image"),
        lambda c: c.pop("mcp-tools"),
        lambda c: c.update({"mcp-tools": []}),
        lambda c: c.update({"mcp-tools": [{"tool-name": "x"}]}),
        lambda c: c.update({"mcp-tools": [{"tool-name": "x", "arg-name": "q"}, {"tool-name": "x", "arg-name": "r"}]}),
        lambda c: c.update({"passthrough-tools": "list_tables"}),
        lambda c: c.update({"passthrough-tools": ["read_query"]}),  # both guarded and passed through
        lambda c: c.pop("sql-data-guard"),
        lambda c: c["sql-data-guard"].pop("tables"),
        lambda c: c["sql-data-guard"].update({"dialect": 5}),
    ],
)
def test_invalid_configuration_is_rejected(mutate):
    config = _config()
    mutate(config)
    with pytest.raises(ValueError):
        wrapper.validate_config(config)


def test_configuration_that_is_not_an_object():
    with pytest.raises(ValueError):
        wrapper.validate_config([])


def test_main_reports_a_missing_configuration(monkeypatch, capsys):
    monkeypatch.setenv("SDG_CONFIG", "/nonexistent/config.json")
    assert wrapper.main() == 2
    assert "Invalid SQL Data Guard configuration" in capsys.readouterr().err


def test_load_config_reads_the_file_named_by_the_environment(monkeypatch, tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps(_config()))
    monkeypatch.setenv("SDG_CONFIG", str(path))
    assert wrapper.load_config()["mcp-server"]["image"] == "mcp/sqlite"


# ------------------------------------------------------------------ the run-time loop, with a fake Docker
class _FakeSocket:
    def __init__(self, sent):
        self._sock = self
        self.sent = sent

    def setblocking(self, flag):
        pass

    def sendall(self, data):
        self.sent.append(data)


class _FakeContainer:
    def __init__(self, output_chunks, sent, wait_for_request=False):
        self._chunks = output_chunks
        self._sent = sent
        self._wait_for_request = wait_for_request
        self.stopped = False

    def logs(self, stream=True):
        # a real server only answers after it received a request
        import time

        deadline = time.time() + 2
        while self._wait_for_request and not self._sent and time.time() < deadline:
            time.sleep(0.01)
        return iter(self._chunks)

    def attach_socket(self, params=None):
        return _FakeSocket(self._sent)

    def stop(self):
        self.stopped = True


def _run_main(monkeypatch, tmp_path, stdin_lines, output_chunks, config=None):
    wait_for_request = bool(stdin_lines)
    import io
    import sys
    import threading
    import types

    sent = []
    container = _FakeContainer(output_chunks, sent, wait_for_request)
    fake_docker = types.ModuleType("docker")
    started = {}

    def run(image, args, **kwargs):
        started.update(image=image, args=args, **kwargs)
        return container

    fake_docker.from_env = lambda: types.SimpleNamespace(containers=types.SimpleNamespace(run=run))
    monkeypatch.setitem(sys.modules, "docker", fake_docker)
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config or _config()))
    monkeypatch.setenv("SDG_CONFIG", str(path))
    monkeypatch.setattr(sys, "stdin", io.StringIO("".join(stdin_lines)))
    out, err = io.StringIO(), io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", err)
    assert wrapper.main() == 0
    for thread in threading.enumerate():  # let the output thread drain the fake log
        if thread is not threading.current_thread() and thread.daemon:
            thread.join(timeout=2)
    return sent, out.getvalue(), err.getvalue(), container, started


def test_run_loop_forwards_verified_calls_and_answers_blocked_ones(monkeypatch, tmp_path):
    lines = [
        json.dumps({"jsonrpc": "2.0", "id": 0, "method": "initialize", "params": {}}) + "\n",
        _call("SELECT secret FROM countries", request_id=1) + "\n",
        _call("SELECT name FROM countries", request_id=2) + "\n",
        "garbage\n",
    ]
    sent, out, err, container, started = _run_main(monkeypatch, tmp_path, lines, [])
    forwarded = [json.loads(s) for s in sent]
    assert [m.get("id") for m in forwarded] == [0, 2]  # the blocked call (id 1) never reached the server
    assert forwarded[1]["params"]["arguments"]["query"].endswith("region = 'Asia'")
    answers = [json.loads(line) for line in out.splitlines()]
    assert {a["id"] for a in answers if "result" in a and a["result"].get("isError")} == {1}
    assert any(a.get("error", {}).get("code") == -32700 for a in answers)
    assert container.stopped and started["image"] == "mcp/sqlite"


def test_run_loop_keeps_the_client_stream_clean(monkeypatch, tmp_path):
    # chunks cut a message in two, join two messages, and carry a log line and a server notification
    chunks = [
        b'{"jsonrpc": "2.0", "id": 5, "res',
        b'ult": {"content": []}}\nserver ready\n{"jsonrpc": "2.0", "method": "notifications/x"}\n',
        b'{"jsonrpc": "2.0", "id": 6, "result": {}}',
    ]
    _, out, err, _, _ = _run_main(monkeypatch, tmp_path, [], chunks)
    stdout_messages = [json.loads(line) for line in out.splitlines()]
    assert [m.get("id") for m in stdout_messages] == [5, None, 6]
    assert "server ready" in err and "server ready" not in out


def test_run_loop_annotates_the_response_of_a_fixed_query(monkeypatch, tmp_path):
    config = _config()
    config["sql-data-guard"]["inject-response"] = True
    response = json.dumps({"jsonrpc": "2.0", "id": 3, "result": {"content": [{"type": "text", "text": "rows"}]}})
    _, out, _, _, _ = _run_main(
        monkeypatch, tmp_path, [_call("SELECT name FROM countries", request_id=3) + "\n"], [response.encode() + b"\n"], config
    )
    content = json.loads(out.splitlines()[0])["result"]["content"]
    assert len(content) == 2 and "SQL Data Guard" in content[0]["text"] and content[1]["text"] == "rows"
