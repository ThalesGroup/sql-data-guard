import json
import os
import sys
from unittest.mock import MagicMock, patch

# Dynamically mock dify_plugin if not available
try:
    import dify_plugin
except ImportError:
    class MockTool:
        def create_variable_message(self, name, value):
            return {"name": name, "value": value}

    mock_dify = MagicMock()
    mock_dify.Tool = MockTool
    sys.modules["dify_plugin"] = mock_dify
    sys.modules["dify_plugin.entities.tool"] = mock_dify

# Now import the tool
from plugins.dify.tools.sql_data_guard_tool import SqlDataGuardTool, _send_telemetry_payload


class TestDifyTelemetryUnit:
    @patch("urllib.request.urlopen")
    def test_send_telemetry_payload_success(self, mock_urlopen):
        # Arrange
        mock_response = MagicMock()
        mock_response.read.return_value = b"OK"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # Act
        _send_telemetry_payload("http://test.url", "postgres", "allowed", None)

        # Assert
        mock_urlopen.assert_called_once()
        args, kwargs = mock_urlopen.call_args
        req = args[0]
        assert req.full_url == "http://test.url"
        assert req.method == "POST"
        assert req.headers["Content-type"] == "application/json"

        # Verify JSON content
        data = json.loads(req.data.decode("utf-8"))
        assert data["dialect"] == "postgres"
        assert data["result"] == "allowed"
        assert "violation_type" not in data

    @patch("urllib.request.urlopen")
    def test_send_telemetry_payload_with_violation(self, mock_urlopen):
        # Arrange
        mock_response = MagicMock()
        mock_response.read.return_value = b"OK"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # Act
        _send_telemetry_payload("http://test.url", "mysql", "blocked", "column")

        # Assert
        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        data = json.loads(req.data.decode("utf-8"))
        assert data["dialect"] == "mysql"
        assert data["result"] == "blocked"
        assert data["violation_type"] == "column"

    @patch("urllib.request.urlopen")
    def test_send_telemetry_payload_silently_ignores_error(self, mock_urlopen):
        # Arrange
        mock_urlopen.side_effect = Exception("Network error")

        # Act & Assert
        try:
            _send_telemetry_payload("http://test.url", "sqlite", "fixed", "table")
        except Exception as e:
            assert False, f"Exception should have been caught silently but raised: {e}"

    def test_trigger_telemetry_allowed(self):
        # Arrange
        tool = SqlDataGuardTool()
        result = {
            "allowed": True,
            "errors": set(),
            "fixed": None,
            "risk": 0.0
        }

        with patch.object(tool, "_trigger_telemetry") as mock_trigger:
            list(tool._invoke({
                "sql": "SELECT id FROM users",
                "config": '{"tables": [{"table_name": "users", "columns": ["id"]}]}',
                "dialect": "postgres",
                "enable_telemetry": True
            }))
            mock_trigger.assert_called_once_with("postgres", result)

    @patch("threading.Thread")
    def test_trigger_telemetry_classification_allowed(self, mock_thread):
        # Arrange
        tool = SqlDataGuardTool()
        result = {
            "allowed": True,
            "errors": [],
            "fixed": None
        }

        # Act
        with patch.dict(os.environ, {"SQL_DATA_GUARD_TELEMETRY_URL": "http://env.url"}):
            tool._trigger_telemetry("postgres", result)

        # Assert
        mock_thread.assert_called_once()
        args, kwargs = mock_thread.call_args
        assert kwargs["daemon"] is True
        assert kwargs["target"] == _send_telemetry_payload
        assert kwargs["args"] == ("http://env.url", "postgres", "allowed", None)

    @patch("threading.Thread")
    def test_trigger_telemetry_classification_fixed_column(self, mock_thread):
        # Arrange
        tool = SqlDataGuardTool()
        result = {
            "allowed": False,
            "errors": ["Column name not allowed. Column removed from SELECT clause"],
            "fixed": "SELECT id FROM orders"
        }

        # Act
        with patch.dict(os.environ, {}, clear=True):
            tool._trigger_telemetry("mysql", result)

        # Assert
        mock_thread.assert_called_once()
        args, kwargs = mock_thread.call_args
        assert kwargs["args"] == (
            "https://telemetry.sql-data-guard.org/metrics",
            "mysql",
            "fixed",
            "column"
        )

    @patch("threading.Thread")
    def test_trigger_telemetry_classification_blocked_table(self, mock_thread):
        # Arrange
        tool = SqlDataGuardTool()
        result = {
            "allowed": False,
            "errors": ["Table users is not allowed"],
            "fixed": None
        }

        # Act
        tool._trigger_telemetry("sqlite", result)

        # Assert
        mock_thread.assert_called_once()
        args, kwargs = mock_thread.call_args
        assert kwargs["args"] == (
            "https://telemetry.sql-data-guard.org/metrics",
            "sqlite",
            "blocked",
            "table"
        )

    @patch("threading.Thread")
    def test_trigger_telemetry_classification_blocked_other(self, mock_thread):
        # Arrange
        tool = SqlDataGuardTool()
        result = {
            "allowed": False,
            "errors": ["Always-True expression is not allowed"],
            "fixed": None
        }

        # Act
        tool._trigger_telemetry(None, result)

        # Assert
        mock_thread.assert_called_once()
        args, kwargs = mock_thread.call_args
        assert kwargs["args"] == (
            "https://telemetry.sql-data-guard.org/metrics",
            None,
            "blocked",
            "other"
        )
