"""
Cypher/Neo4j query verification guard.

This module provides custom, dependency-free parsing and validation for Cypher
queries, allowing SQL Data Guard to enforce read-only statements, whitelist node labels/properties,
and verify property value restrictions.
"""

import re
from typing import Any


def tokenize_and_strip(query: str) -> tuple[str, list[str]]:
    """
    Strips single/multi-line comments and replaces string literals with placeholders.

    Args:
        query (str): The raw Cypher query.

    Returns:
        tuple: (cleaned_query, list of original string literals)
    """
    strings = []
    cleaned = []
    i = 0
    n = len(query)
    while i < n:
        # Check single-line comment
        if i + 1 < n and query[i : i + 2] == "//":
            i += 2
            while i < n and query[i] != "\n":
                i += 1
            cleaned.append(" ")
        # Check block comment
        elif i + 1 < n and query[i : i + 2] == "/*":
            i += 2
            while i + 1 < n and query[i : i + 2] != "*/":
                i += 1
            i += 2
            cleaned.append(" ")
        # Check string literal
        elif query[i] in ("'", '"'):
            quote_char = query[i]
            start = i
            i += 1
            escaped = False
            while i < n:
                if escaped:
                    escaped = False
                elif query[i] == "\\":
                    escaped = True
                elif query[i] == quote_char:
                    break
                i += 1
            val = query[start : i + 1]
            placeholder = f"$STR_{len(strings)}$"
            strings.append(val)
            cleaned.append(placeholder)
            i += 1
        else:
            cleaned.append(query[i])
            i += 1
    return "".join(cleaned), strings


def parse_inline_map(map_content: str) -> dict[str, str]:
    """
    Parses properties and values from an inline map string.

    Args:
        map_content (str): The inner content of the curly braces.

    Returns:
        dict: A dictionary of property keys and raw values.
    """
    pairs = {}
    for m in re.finditer(r"\b([a-zA-Z0-9_]+)\s*:\s*([^,]+)", map_content):
        key = m.group(1).strip()
        val = m.group(2).strip()
        pairs[key] = val
    return pairs


def parse_patterns(cleaned_query: str) -> tuple[dict[str, list[str]], list[tuple[list[str], str]]]:
    """
    Extracts node and relationship variables, mapping them to labels, and extracts inline map properties.

    Args:
        cleaned_query (str): The comment/string stripped Cypher query.

    Returns:
        tuple: (variable_to_labels_map, list of (labels, property_key) from inline maps)
    """
    var_to_labels: dict[str, list[str]] = {}
    inline_properties = []

    # 1. Parse node patterns: (variable:Label1:Label2 {inlineMap})
    node_regex = re.compile(r"\(\s*([a-zA-Z0-9_]+)?\s*((?::[a-zA-Z0-9_]+)+)?\s*(\{([^}]+)\})?\s*\)")
    for match in node_regex.finditer(cleaned_query):
        var_name = match.group(1)
        labels_str = match.group(2)
        map_content = match.group(4)

        labels = []
        if labels_str:
            labels = [lbl for lbl in labels_str.split(":") if lbl]

        if var_name:
            if var_name not in var_to_labels:
                var_to_labels[var_name] = []
            for label in labels:
                if label not in var_to_labels[var_name]:
                    var_to_labels[var_name].append(label)

        if map_content:
            keys = re.findall(r"\b([a-zA-Z0-9_]+)\s*:", map_content)
            for key in keys:
                inline_properties.append((labels, key))

    # 2. Parse relationship patterns: -[variable:REL_TYPE {inlineMap}]->
    rel_regex = re.compile(r"\[\s*([a-zA-Z0-9_]+)?\s*((?::[a-zA-Z0-9_]+)+)?\s*(\{([^}]+)\})?\s*\]")
    for match in rel_regex.finditer(cleaned_query):
        var_name = match.group(1)
        labels_str = match.group(2)
        map_content = match.group(4)

        labels = []
        if labels_str:
            labels = [lbl for lbl in labels_str.split(":") if lbl]

        if var_name:
            if var_name not in var_to_labels:
                var_to_labels[var_name] = []
            for label in labels:
                if label not in var_to_labels[var_name]:
                    var_to_labels[var_name].append(label)

        if map_content:
            keys = re.findall(r"\b([a-zA-Z0-9_]+)\s*:", map_content)
            for key in keys:
                inline_properties.append((labels, key))

    return var_to_labels, inline_properties


def _is_restriction_satisfied(
    cleaned_query: str,
    strings: list[str],
    var_to_labels: dict[str, list[str]],
    table_name: str,
    column: str,
    expected_value: Any,
) -> bool:
    """
    Checks if the cleaned query has a matching filter satisfying the given restriction.

    Args:
        cleaned_query (str): Stripped Cypher query.
        strings (list): Original string literals.
        var_to_labels (dict): Mapped variables.
        table_name (str): Label of node/type of relationship.
        column (str): Restricted property key.
        expected_value (Any): Target restriction value.

    Returns:
        bool: True if restriction is satisfied, False otherwise.
    """
    expected_str = expected_value.strip("'\"") if isinstance(expected_value, str) else str(expected_value)

    # Case 1: Check inline maps: e.g. (u:User {accountId: 123})
    node_regex = re.compile(r"\(\s*([a-zA-Z0-9_]+)?\s*((?::[a-zA-Z0-9_]+)+)?\s*(\{([^}]+)\})?\s*\)")
    for match in node_regex.finditer(cleaned_query):
        var_name = match.group(1)
        labels_str = match.group(2)
        map_content = match.group(4)

        labels = [lbl for lbl in labels_str.split(":") if lbl] if labels_str else []
        if var_name and var_name in var_to_labels and not labels:
            labels = var_to_labels[var_name]

        if table_name in labels and map_content:
            pairs = parse_inline_map(map_content)
            if column in pairs:
                val = pairs[column]
                if val.startswith("$STR_") and val.endswith("$"):
                    try:
                        idx = int(val[5:-1])
                        val = strings[idx].strip("'\"")
                    except (ValueError, IndexError, TypeError):
                        pass
                if val == expected_str:
                    return True

    rel_regex = re.compile(r"\[\s*([a-zA-Z0-9_]+)?\s*((?::[a-zA-Z0-9_]+)+)?\s*(\{([^}]+)\})?\s*\]")
    for match in rel_regex.finditer(cleaned_query):
        var_name = match.group(1)
        labels_str = match.group(2)
        map_content = match.group(4)

        labels = [lbl for lbl in labels_str.split(":") if lbl] if labels_str else []
        if var_name and var_name in var_to_labels and not labels:
            labels = var_to_labels[var_name]

        if table_name in labels and map_content:
            pairs = parse_inline_map(map_content)
            if column in pairs:
                val = pairs[column]
                if val.startswith("$STR_") and val.endswith("$"):
                    try:
                        idx = int(val[5:-1])
                        val = strings[idx].strip("'\"")
                    except (ValueError, IndexError, TypeError):
                        pass
                if val == expected_str:
                    return True

    # Case 2: Check dot notation filters: e.g. u.accountId = 123
    for var_name, labels in var_to_labels.items():
        if table_name in labels:
            # Match u.accountId = 123
            pattern1 = rf"\b{var_name}\.{column}\s*(?:=|==)\s*([^\s&|)]+)"
            for m in re.finditer(pattern1, cleaned_query, re.IGNORECASE):
                val = m.group(1).strip()
                if val.startswith("$STR_") and val.endswith("$"):
                    try:
                        idx = int(val[5:-1])
                        val = strings[idx].strip("'\"")
                    except (ValueError, IndexError, TypeError):
                        pass
                if val.strip("'\"") == expected_str:
                    return True

            # Match 123 = u.accountId
            pattern2 = rf"([^\s&|(=]+)\s*(?:=|==)\s*\b{var_name}\.{column}\b"
            for m in re.finditer(pattern2, cleaned_query, re.IGNORECASE):
                val = m.group(1).strip()
                if val.startswith("$STR_") and val.endswith("$"):
                    try:
                        idx = int(val[5:-1])
                        val = strings[idx].strip("'\"")
                    except (ValueError, IndexError, TypeError):
                        pass
                if val.strip("'\"") == expected_str:
                    return True

    return False


def verify_cypher(sql: str, config: dict[str, Any]) -> dict[str, Any]:
    """
    Verifies a Cypher query against a given configuration.

    Args:
        sql (str): The Cypher query to verify.
        config (dict): The configuration specifying allowed tables, columns, and restrictions.

    Returns:
        dict: A dictionary containing:
            - "allowed" (bool): Whether the query is allowed to run.
            - "errors" (List[str]): List of errors found during verification.
            - "fixed" (Optional[str]): The fixed query (always None for Cypher).
            - "risk" (float): Verification risk score (0.0 to 1.0)
    """
    errors = []

    # 1. Verify basic config
    if not config or not isinstance(config, dict) or "tables" not in config:
        return {
            "allowed": False,
            "errors": ["Invalid configuration provided. The configuration must include 'tables'."],
            "fixed": None,
            "risk": 1.0,
        }

    # Check max length
    max_length = config.get("max_length", 10_000)
    if len(sql) > max_length:
        return {
            "allowed": False,
            "errors": [f"SQL exceeds maximum length of {max_length} characters."],
            "fixed": None,
            "risk": 1.0,
        }

    # 2. Tokenize and strip comments & strings
    cleaned_query, strings = tokenize_and_strip(sql)

    # 3. Check for mutating statements outside string literals and comments
    mutations = re.findall(r"\b(CREATE|DELETE|DETACH|SET|REMOVE|MERGE)\b", cleaned_query, re.IGNORECASE)
    if mutations:
        for mut in sorted(set(mutations)):
            errors.append(f"{mut.upper()} statement is not allowed")

    # 4. Extract node labels and relationship types
    var_to_labels, inline_properties = parse_patterns(cleaned_query)

    # Collect all queried labels
    queried_labels = set()
    for labels in var_to_labels.values():
        for label in labels:
            queried_labels.add(label)
    for labels, _ in inline_properties:
        for label in labels:
            queried_labels.add(label)

    # 5. Validate labels against config "tables"
    allowed_tables = {t["table_name"] for t in config["tables"]}
    for label in sorted(queried_labels):
        if label not in allowed_tables:
            errors.append(f"Table {label} is not allowed")

    # 6. Extract dot-notation property accesses: e.g. u.accountId
    dot_properties = []
    dot_regex = re.compile(r"\b([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b")
    for match in dot_regex.finditer(cleaned_query):
        var_name = match.group(1)
        prop_name = match.group(2)
        if var_name == "STR" or prop_name.startswith("STR_"):
            continue
        dot_properties.append((var_name, prop_name))

    # 7. Validate all extracted properties against allowed "columns"
    table_columns = {t["table_name"]: set(t.get("columns", [])) for t in config["tables"]}

    # Validate inline map properties
    for labels, prop_key in inline_properties:
        if labels:
            allowed_for_any_label = False
            for label in labels:
                if label in table_columns and prop_key in table_columns[label]:
                    allowed_for_any_label = True
                    break
            if not allowed_for_any_label:
                err = f"Column {prop_key} is not allowed"
                if err not in errors:
                    errors.append(err)
        else:
            allowed_somewhere = False
            for allowed_cols in table_columns.values():
                if prop_key in allowed_cols:
                    allowed_somewhere = True
                    break
            if not allowed_somewhere:
                err = f"Column {prop_key} is not allowed"
                if err not in errors:
                    errors.append(err)

    # Validate dot notation properties
    for var_name, prop_key in dot_properties:
        if var_name in var_to_labels and var_to_labels[var_name]:
            labels = var_to_labels[var_name]
            allowed_for_any_label = False
            for label in labels:
                if label in table_columns and prop_key in table_columns[label]:
                    allowed_for_any_label = True
                    break
            if not allowed_for_any_label:
                err = f"Column {prop_key} is not allowed"
                if err not in errors:
                    errors.append(err)
        else:
            allowed_somewhere = False
            for allowed_cols in table_columns.values():
                if prop_key in allowed_cols:
                    allowed_somewhere = True
                    break
            if not allowed_somewhere:
                err = f"Column {prop_key} is not allowed"
                if err not in errors:
                    errors.append(err)

    # 8. Verify restrictions
    for t in config["tables"]:
        table_name = t["table_name"]
        if table_name in queried_labels:
            for rest in t.get("restrictions", []):
                column = rest["column"]
                expected_value = rest.get("value")
                satisfied = _is_restriction_satisfied(
                    cleaned_query, strings, var_to_labels, table_name, column, expected_value
                )
                if not satisfied:
                    val_desc = rest.get("values", rest.get("value"))
                    err = f"Missing restriction for table: {table_name} column: {column} value: {val_desc}"
                    if err not in errors:
                        errors.append(err)

    allowed = len(errors) == 0
    return {
        "allowed": allowed,
        "errors": errors,
        "fixed": None,
        "risk": 1.0 if not allowed else 0.0,
    }
