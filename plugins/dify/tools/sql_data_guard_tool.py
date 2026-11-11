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
        dialect = tool_parameters.get("dialect") or None

        if not sql:
            raise ValueError("Missing required parameter 'sql'")
        if not config:
            raise ValueError("Missing required parameter 'config'")
        if isinstance(config, dict):  # a workflow variable that already holds the object
            config_dict = config
        else:
            try:
                config_dict = json.loads(config)
            except json.decoder.JSONDecodeError as e:
                raise ValueError(f"Invalid config JSON: {e}") from e
        if not isinstance(config_dict, dict):
            raise ValueError("The policy configuration must be a JSON object")

        result = verify_sql(sql, config_dict, dialect)
        allowed = bool(result.get("allowed", False))
        fixed = result.get("fixed")
        yield self.create_variable_message("allowed", allowed)
        yield self.create_variable_message("fixed_sql", fixed)
        # older releases of the library return a set, which cannot be sent as a variable
        yield self.create_variable_message("errors", sorted(result.get("errors", [])))
        yield self.create_variable_message("risk", result.get("risk"))
        yield self.create_variable_message("verified_sql", sql if allowed else fixed)
