import logging
import os
from logging.config import fileConfig

from flask import Flask, jsonify, request

from sql_data_guard import verify_sql

app = Flask(__name__)


@app.route("/verify-sql", methods=["POST"])
def _verify_sql():
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
