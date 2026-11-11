"""The Dify tool, tested with a stub of the Dify SDK (the SDK is not a dependency of the project)."""

import importlib.util
import os
import re
import sys
import types

import pytest

_PLUGIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "plugins", "dify")

CONFIG = (
    '{"tables": [{"table_name": "orders", "columns": ["id", "product_name", "account_id"], '
    '"restrictions": [{"column": "account_id", "value": 123}]}]}'
)


class _Tool:
    def create_variable_message(self, name, value):
        return (name, value)


@pytest.fixture
def module(monkeypatch):
    sdk = types.ModuleType("dify_plugin")
    sdk.Tool = _Tool
    entities = types.ModuleType("dify_plugin.entities")
    entities_tool = types.ModuleType("dify_plugin.entities.tool")
    entities_tool.ToolInvokeMessage = object
    monkeypatch.setitem(sys.modules, "dify_plugin", sdk)
    monkeypatch.setitem(sys.modules, "dify_plugin.entities", entities)
    monkeypatch.setitem(sys.modules, "dify_plugin.entities.tool", entities_tool)
    spec = importlib.util.spec_from_file_location(
        "sql_data_guard_tool_under_test", os.path.join(_PLUGIN, "tools", "sql_data_guard_tool.py")
    )
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def _run(module, **params):
    return dict(module.SqlDataGuardTool()._invoke(params))


def test_allowed_query(module):
    out = _run(module, sql="SELECT id FROM orders WHERE account_id = 123", config=CONFIG, dialect="sqlite")
    assert out == {
        "allowed": True,
        "fixed_sql": None,
        "errors": [],
        "risk": 0,
        "verified_sql": "SELECT id FROM orders WHERE account_id = 123",
    }


def test_fixable_query_returns_the_fixed_sql_as_verified_sql(module):
    out = _run(module, sql="SELECT id, secret FROM orders", config=CONFIG, dialect="sqlite")
    assert out["allowed"] is False
    assert out["fixed_sql"] == "SELECT id FROM orders WHERE account_id = 123"
    assert out["verified_sql"] == out["fixed_sql"]
    assert isinstance(out["errors"], list) and out["errors"] == sorted(out["errors"])


def test_unfixable_query_has_no_verified_sql(module):
    out = _run(module, sql="SELECT id FROM orders; DROP TABLE orders", config=CONFIG, dialect="sqlite")
    assert out["allowed"] is False and out["fixed_sql"] is None and out["verified_sql"] is None
    assert out["errors"] == ["Multiple statements are not allowed"]


def test_errors_of_an_older_library_release_are_sent_as_a_list(module, monkeypatch):
    # releases up to 0.1.9 return the errors as a set, which Dify cannot send as a variable
    monkeypatch.setattr(
        module,
        "verify_sql",
        lambda sql, config, dialect: {"allowed": False, "errors": {"b", "a"}, "fixed": None, "risk": 1},
    )
    out = _run(module, sql="SELECT 1", config=CONFIG)
    assert out["errors"] == ["a", "b"]


def test_empty_dialect_means_the_default_dialect(module, monkeypatch):
    seen = []
    monkeypatch.setattr(
        module,
        "verify_sql",
        lambda sql, config, dialect: seen.append(dialect) or {"allowed": True, "errors": [], "fixed": None, "risk": 0},
    )
    _run(module, sql="SELECT 1", config=CONFIG, dialect="")
    _run(module, sql="SELECT 1", config=CONFIG)
    assert seen == [None, None]


def test_unknown_dialect_is_reported_as_an_error(module):
    out = _run(module, sql="SELECT id FROM orders", config=CONFIG, dialect="nope")
    assert out["allowed"] is False and "nope" in out["errors"][0]


def test_config_that_is_already_an_object(module):
    import json

    out = _run(module, sql="SELECT id FROM orders WHERE account_id = 123", config=json.loads(CONFIG))
    assert out["allowed"] is True


@pytest.mark.parametrize(
    "params,message",
    [
        ({"config": CONFIG}, "Missing required parameter 'sql'"),
        ({"sql": ""}, "Missing required parameter 'sql'"),
        ({"sql": "SELECT 1"}, "Missing required parameter 'config'"),
        ({"sql": "SELECT 1", "config": ""}, "Missing required parameter 'config'"),
        ({"sql": "SELECT 1", "config": "{not json"}, "Invalid config JSON"),
        ({"sql": "SELECT 1", "config": "[1, 2]"}, "must be a JSON object"),
        ({"sql": "SELECT 1", "config": '"text"'}, "must be a JSON object"),
    ],
)
def test_invalid_input(module, params, message):
    with pytest.raises(ValueError, match=message):
        _run(module, **params)


def test_output_variables_match_the_declared_output_schema(module):
    with open(os.path.join(_PLUGIN, "tools", "sql_data_guard_tool.yaml")) as f:
        schema = f.read().split("output_schema:")[1]
    declared = set(re.findall(r"^    (\w+):$", schema, flags=re.M))
    out = _run(module, sql="SELECT id FROM orders WHERE account_id = 123", config=CONFIG, dialect="sqlite")
    assert set(out) == declared


def test_the_plugin_sends_nothing_over_the_network():
    # no telemetry: the tool must not import anything that opens connections
    with open(os.path.join(_PLUGIN, "tools", "sql_data_guard_tool.py")) as f:
        source = f.read()
    for forbidden in ("urllib", "requests", "http.client", "socket", "httpx", "aiohttp"):
        assert forbidden not in source
