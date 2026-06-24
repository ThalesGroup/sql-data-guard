import json
import os
import threading
import urllib.request
from collections.abc import Generator
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

from sql_data_guard import verify_sql


def _send_telemetry_payload(url: str, dialect: str | None, result_type: str, violation_type: str | None):
    payload = {
        "dialect": dialect or "unknown",
        "result": result_type,
    }
    if violation_type:
        payload["violation_type"] = violation_type

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=2.0) as response:
            response.read()
    except Exception:
        pass


class SqlDataGuardTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        sql = tool_parameters.get("sql")
        config = tool_parameters.get("config")
        dialect = tool_parameters.get("dialect")
        enable_telemetry = tool_parameters.get("enable_telemetry", False)

        if not sql:
            raise ValueError("Missing required parameter 'sql'")
        if not config:
            raise ValueError("Missing required parameter 'config'")
        try:
            config_dict = json.loads(config)
        except json.decoder.JSONDecodeError as e:
            raise ValueError(f"Invalid config JSON: {e}")

        result = verify_sql(sql, config_dict, dialect)
        yield self.create_variable_message("allowed", result.get("allowed", False))
        yield self.create_variable_message("fixed_sql", result.get("fixed"))
        yield self.create_variable_message("errors", result.get("errors", []))
        yield self.create_variable_message("risk", result.get("risk"))
        yield self.create_variable_message(
            "verified_sql", sql if result.get("allowed", False) else result.get("fixed")
        )

        if enable_telemetry:
            self._trigger_telemetry(dialect, result)

    def _trigger_telemetry(self, dialect: str | None, result: dict[str, Any]):
        allowed = result.get("allowed", False)
        fixed = result.get("fixed")
        errors = result.get("errors", [])

        # 1. Determine result category
        if allowed:
            result_type = "allowed"
            violation_type = None
        elif fixed is not None:
            result_type = "fixed"
        else:
            result_type = "blocked"

        # 2. Determine violation type if any violation occurred
        if not allowed:
            violation_type = "other"
            errors_str = " ".join(errors).lower()
            if "column" in errors_str or "select *" in errors_str:
                violation_type = "column"
            elif "table" in errors_str:
                violation_type = "table"
        else:
            violation_type = None

        # 3. Lookup telemetry URL
        url = os.environ.get(
            "SQL_DATA_GUARD_TELEMETRY_URL",
            "https://telemetry.sql-data-guard.org/metrics"
        )

        # 4. Dispatch via daemon background thread
        thread = threading.Thread(
            target=_send_telemetry_payload,
            args=(url, dialect, result_type, violation_type),
            daemon=True
        )
        thread.start()
