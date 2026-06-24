"""
SQL Data Guard FastAPI web application.

This module implements the FastAPI endpoints and logging initialization for the
REST API.
"""

import logging
import os
from logging.config import fileConfig
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from sql_data_guard import verify_sql

app = FastAPI(
    title="SQL Data Guard REST API",
    description="Safety Layer for LLM Database Interactions",
    version="0.0.1",
)


class VerifySQLRequest(BaseModel):
    """
    Request model for the SQL verification endpoint.

    Attributes:
        sql (str): The SQL query string to verify.
        config (dict[str, Any]): The rules and restrictions configuration.
        dialect (str | None): Optional SQL dialect for parsing.
    """

    sql: str = Field(..., description="The SQL query to verify")
    config: dict[str, Any] = Field(
        ...,
        description="The verification configuration specifying allowed tables, columns, and restrictions",
    )
    dialect: str | None = Field(
        None, description="Optional SQL dialect for parsing"
    )


@app.post("/verify-sql")
def _verify_sql(payload: VerifySQLRequest) -> dict[str, Any]:
    """
    HTTP POST endpoint to verify an SQL query against the provided configuration.

    Args:
        payload (VerifySQLRequest): The request payload containing SQL and configuration.

    Returns:
        dict[str, Any]: Verification results including allowed status, errors, fixed query, and risk score.
    """
    result = verify_sql(payload.sql, payload.config, payload.dialect)
    result["errors"] = list(result["errors"])
    return result


def _init_logging() -> None:
    """
    Initializes system logging configuration using the logging.conf file if available.
    """
    log_config_path = Path(__file__).resolve().parent / "logging.conf"
    if log_config_path.exists():
        fileConfig(str(log_config_path))
        logging.info("Logging initialized")
    else:
        logging.basicConfig(level=logging.INFO)
        logging.info("Logging initialized with basic configuration")


if __name__ == "__main__":
    _init_logging()
    import uvicorn

    port = int(os.environ.get("APP_PORT", 5000))
    logging.info(f"Going to start the app. Port: {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)  # noqa: S104
