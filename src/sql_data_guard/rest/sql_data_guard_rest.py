import logging
import os
from logging.config import fileConfig

from flasgger import Swagger
from flask import Flask, jsonify, request

from sql_data_guard import verify_sql

app = Flask(__name__)
swagger = Swagger(
    app,
    template={
        "info": {
            "title": "sql-data-guard API",
            "description": "Safety Layer for LLM Database Interactions. "
            "Verifies and optionally rewrites SQL queries against a "
            "restriction configuration.",
            "version": "1.0",
        }
    },
)

# Optional API-key authentication (S6). Disabled unless SQL_GUARD_API_KEY is set,
# preserving the default open behavior for local/dev use.
_API_KEY = os.environ.get("SQL_GUARD_API_KEY")


@app.before_request
def _require_api_key():
    if _API_KEY and request.headers.get("X-API-Key") != _API_KEY:
        return jsonify({"error": "Unauthorized"}), 401


@app.route("/verify-sql", methods=["POST"])
def _verify_sql():
    """Verify an SQL query against a restriction configuration.
    ---
    consumes:
      - application/json
    produces:
      - application/json
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - sql
            - config
          properties:
            sql:
              type: string
              description: The SQL query to verify.
              example: SELECT * FROM orders WHERE account_id = 123
            config:
              type: object
              description: >-
                The restriction configuration: allowed tables, columns,
                row restrictions, and optional column_masks (redact | hash |
                partial) that rewrite sensitive columns instead of dropping them.
              example:
                tables:
                  - table_name: orders
                    columns:
                      - id
                      - product_name
                      - account_id
                      - credit_card
                    restrictions:
                      - column: account_id
                        value: 123
                    column_masks:
                      - column: credit_card
                        policy: partial
                        show_last: 4
            dialect:
              type: string
              description: Optional SQL dialect (e.g. "sqlite", "postgres").
              example: sqlite
    responses:
      200:
        description: Verification result.
        schema:
          type: object
          properties:
            allowed:
              type: boolean
              description: Whether the query is allowed to run.
            errors:
              type: array
              items:
                type: string
              description: List of restriction violations found.
            fixed:
              type: string
              description: A rewritten, compliant query (null if none needed).
            risk:
              type: number
              description: Risk score of the query.
      400:
        description: Bad request (missing or invalid input).
        schema:
          type: object
          properties:
            error:
              type: string
    """
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    data = request.get_json()
    if "sql" not in data:
        return jsonify({"error": "Missing 'sql' in request"}), 400
    sql = data["sql"]
    if "config" not in data:
        return jsonify({"error": "Missing 'config' in request"}), 400
    config = data["config"]
    dialect = data.get("dialect")
    result = verify_sql(sql, config, dialect)
    result["errors"] = list(result["errors"])
    return jsonify(result)


def _init_logging():
    fileConfig(os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging.conf"))
    logging.info("Logging initialized")


if __name__ == "__main__":
    _init_logging()
    logging.getLogger("werkzeug").setLevel("WARNING")
    port = os.environ.get("APP_PORT", 5000)
    logging.info(f"Going to start the app. Port: {port}")
    app.run(host="0.0.0.0", port=port)
