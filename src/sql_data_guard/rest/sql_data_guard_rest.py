"""
REST service around sql_data_guard.verify_sql.

POST /verify-sql  {"sql": "...", "config": {...}, "dialect": "..."}  ->  {allowed, errors, fixed, risk}

Environment variables:
  APP_HOST               address to bind (default 127.0.0.1; the Docker image sets 0.0.0.0)
  APP_PORT               port (default 5000)
  APP_API_TOKEN          if set, requests must send "Authorization: Bearer <token>"
  APP_MAX_REQUEST_BYTES  maximum request size (default 1,000,000)

The client sends the policy ("config") with each request, so this service is not an enforcement
point against callers who should not be able to choose their own policy.
"""

import hmac
import logging
import os
from logging.config import fileConfig

from flask import Flask, jsonify, request

from sql_data_guard import verify_sql

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("APP_MAX_REQUEST_BYTES", 1_000_000))


@app.before_request
def _check_token():
    token = os.environ.get("APP_API_TOKEN")
    if not token:
        return None
    header = request.headers.get("Authorization", "")
    supplied = header[len("Bearer ") :] if header.startswith("Bearer ") else ""
    if not hmac.compare_digest(supplied.encode("utf-8"), token.encode("utf-8")):
        return jsonify({"error": "Unauthorized"}), 401
    return None


@app.errorhandler(413)
def _too_large(_):
    return jsonify({"error": "Request is too large"}), 413


@app.route("/verify-sql", methods=["POST"])
def _verify_sql():
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Request body must be a JSON object"}), 400
    if "sql" not in data:
        return jsonify({"error": "Missing 'sql' in request"}), 400
    if "config" not in data:
        return jsonify({"error": "Missing 'config' in request"}), 400
    sql, config, dialect = data["sql"], data["config"], data.get("dialect")
    if not isinstance(sql, str):
        return jsonify({"error": "'sql' must be a string"}), 400
    if not isinstance(config, dict):
        return jsonify({"error": "'config' must be a JSON object"}), 400
    if dialect is not None and not isinstance(dialect, str):
        return jsonify({"error": "'dialect' must be a string"}), 400
    result = verify_sql(sql, config, dialect)
    # older releases of the library return a set, which cannot be serialised to JSON; the Docker image
    # pairs this file with the library from PyPI, so it has to work with both
    result["errors"] = sorted(result["errors"])
    return jsonify(result)


def _init_logging():
    fileConfig(os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging.conf"))
    logging.info("Logging initialized")


if __name__ == "__main__":
    _init_logging()
    logging.getLogger("werkzeug").setLevel("WARNING")
    host = os.environ.get("APP_HOST", "127.0.0.1")
    port = int(os.environ.get("APP_PORT", 5000))
    logging.info(f"Going to start the app. Host: {host} Port: {port}")
    if not os.environ.get("APP_API_TOKEN"):
        logging.warning("APP_API_TOKEN is not set: the service accepts unauthenticated requests")
    app.run(host=host, port=port)
