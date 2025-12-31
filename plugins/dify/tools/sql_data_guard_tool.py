import json
from collections.abc import Generator
from typing import Any

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

from sql_data_guard import verify_sql


class SqlDataGuardTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        sql = tool_parameters.get("sql")
        config = tool_parameters.get("config")
        dialect = tool_parameters.get("dialect")

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
