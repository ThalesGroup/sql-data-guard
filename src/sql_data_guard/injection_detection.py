"""Malicious-payload / SQL-injection detection module.

This module implements the "built-in module for detection of malicious payloads"
that the project README advertises. It performs two complementary scans:

1. A *pre-parse* raw-string scan that catches attacks which the AST traversal
   cannot see, most importantly comment-based evasion (``--``, ``/* */``, ``#``)
   that hides a payload tail from sqlglot.
2. An *AST* scan that flags known attack constructs (stacked / multiple
   statements, dangerous functions, system-catalog probing) with
   *intent-revealing* error messages and risk weights, instead of relying on
   incidental rules such as "static expression is not allowed".

The functions here only *report* findings through
:class:`~sql_data_guard.verification_context.VerificationContext`. They never
mutate the query. Stripping/auto-fix remains the responsibility of the core
verifier.
"""

from __future__ import annotations

import re
from typing import List, Pattern, Tuple

import sqlglot.expressions as expr

from .verification_context import VerificationContext

# Risk weights. Kept in named constants so the magic numbers are documented
# in one place and easy to tune.
_RISK_STACKED_QUERY = 0.9
_RISK_COMMENT_EVASION = 0.8
_RISK_DANGEROUS_FUNCTION = 0.9
_RISK_SYSTEM_CATALOG = 0.8

# Functions that are dangerous regardless of the configured allow-list:
# file access, command execution and time-based blind-probe primitives.
_DANGEROUS_FUNCTIONS = frozenset(
    {
        "load_file",
        "load_data",
        "xp_cmdshell",
        "pg_read_file",
        "pg_sleep",
        "sleep",
        "benchmark",
        "waitfor",
        "dbms_pipe",
        "sys_exec",
        "sys_eval",
    }
)

# System catalogs / metadata tables that legitimate application queries should
# never need. Probing them is a classic information-schema exfiltration step.
_SYSTEM_CATALOGS = frozenset(
    {
        "information_schema",
        "sqlite_master",
        "sqlite_temp_master",
        "pg_catalog",
    }
)


# Pre-parse patterns. Each tuple is (compiled_regex, error_message, risk).
# These run on the raw SQL string *before* sqlglot parsing.
_COMMENT_PATTERNS: List[Tuple[Pattern[str], str, float]] = [
    (
        re.compile(r"--"),
        "Comment-based evasion detected: '--' line comment is not allowed",
        _RISK_COMMENT_EVASION,
    ),
    (
        re.compile(r"/\*.*?\*/", re.DOTALL),
        "Comment-based evasion detected: '/* */' block comment is not allowed",
        _RISK_COMMENT_EVASION,
    ),
    (
        re.compile(r"#"),
        "Comment-based evasion detected: '#' comment is not allowed",
        _RISK_COMMENT_EVASION,
    ),
]


def scan_raw_sql(sql: str, context: VerificationContext) -> None:
    """Scan the raw SQL string for attacks invisible to the AST.

    Comment-based evasion is *opt-in* because legitimate queries may contain
    comments. It is only scanned when ``detect_comments`` is enabled in the
    config (see :func:`comment_detection_enabled`).

    Args:
        sql: The raw, unparsed SQL query.
        context: The verification context to report findings to.
    """
    if not comment_detection_enabled(context.config):
        return
    for pattern, message, risk in _COMMENT_PATTERNS:
        if pattern.search(sql):
            context.add_error(message, False, risk)


def scan_parsed_sql(parsed: expr.Expression, context: VerificationContext) -> None:
    """Scan the parsed AST for known malicious constructs.

    Stacked statements, dangerous functions and system-catalog probing are
    *always on* (they have no legitimate use in an application query). Only
    comment detection (in :func:`scan_raw_sql`) is opt-in.

    Args:
        parsed: The root sqlglot expression produced by ``parse_one``.
        context: The verification context to report findings to.
    """
    _scan_stacked_statements(parsed, context)
    _scan_dangerous_functions(parsed, context)
    _scan_system_catalogs(parsed, context)


def comment_detection_enabled(config: dict) -> bool:
    """Return whether opt-in comment-evasion detection is enabled.

    Accepts either ``{"detect_injection": {"comments": True}}`` or the simple
    shorthand ``{"detect_comments": True}``. Defaults to ``False`` so existing
    queries containing benign comments are unaffected.
    """
    if config.get("detect_comments"):
        return True
    detect = config.get("detect_injection")
    if isinstance(detect, dict):
        return bool(detect.get("comments"))
    return False



def is_stacked_statement(parsed: expr.Expression) -> bool:
    """Return ``True`` if ``parsed`` represents more than one SQL statement.

    Newer sqlglot versions wrap stacked statements (``a; b``) in a ``Block``
    node. Detecting this explicitly lets the caller emit an intent-revealing
    error instead of the incidental "could not find a query statement".
    """
    block_cls = getattr(expr, "Block", None)
    if block_cls is not None and isinstance(parsed, block_cls):
        return len(parsed.expressions) > 1
    return False


def _scan_stacked_statements(
    parsed: expr.Expression, context: VerificationContext
) -> None:
    if is_stacked_statement(parsed):
        context.add_error(
            "Stacked query detected: multiple statements are not allowed",
            False,
            _RISK_STACKED_QUERY,
        )


def _scan_dangerous_functions(
    parsed: expr.Expression, context: VerificationContext
) -> None:
    for func in parsed.find_all(expr.Func):
        name = function_name(func)
        if name and name.lower() in _DANGEROUS_FUNCTIONS:
            context.add_error(
                f"Dangerous function detected: {name} is not allowed",
                False,
                _RISK_DANGEROUS_FUNCTION,
            )


def _scan_system_catalogs(
    parsed: expr.Expression, context: VerificationContext
) -> None:
    reported = set()
    for table in parsed.find_all(expr.Table):
        for part in (table.name, table.db, table.catalog):
            if part and part.lower() in _SYSTEM_CATALOGS and part not in reported:
                reported.add(part)
                context.add_error(
                    f"System catalog probing detected: {part} is not allowed",
                    False,
                    _RISK_SYSTEM_CATALOG,
                )


def function_name(func: expr.Func) -> str:
    """Best-effort extraction of a function's name across sqlglot versions.

    Unknown/dialect-specific functions (e.g. ``SLEEP``, ``LOAD_FILE``,
    ``pg_sleep``) are parsed as ``Anonymous`` nodes whose ``sql_names()`` is
    ``["ANONYMOUS"]`` and whose real name is on ``.name``. Built-in functions
    expose the real name through ``sql_names()``. We prefer ``.name`` when the
    only ``sql_names()`` entry is the placeholder ``ANONYMOUS``.
    """
    sql_names = func.sql_names()
    if func.name:
        return func.name
    if sql_names and sql_names[0] != "ANONYMOUS":
        return sql_names[0]
    return type(func).__name__


