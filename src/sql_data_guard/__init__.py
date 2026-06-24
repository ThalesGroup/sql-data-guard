"""
SQL Data Guard package initialization.

This module exposes the main API function verify_sql for verifying SQL queries
against restriction configuration rules.
"""

from .sql_data_guard import verify_sql as verify_sql
