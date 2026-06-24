

"""
SQL Data Guard verification context.

This module defines the VerificationContext class, which maintains the state of
an ongoing SQL verification process, including detected errors, auto-fixes, and risks.
"""

from typing import Any


class VerificationContext:
    """
    Context for verifying SQL queries against a given configuration.

    Attributes:
        _can_fix (bool): Indicates if the query can be fixed.
        _errors (List[str]): List of errors found during verification.
        _fixed (Optional[str]): The fixed query if modifications were made.
        _config (dict): The configuration used for verification.
        _dynamic_tables (Set[str]): Set of dynamic tables found in the query, like sub select and WITH clauses.
        _dialect (str): The SQL dialect to use for parsing.
    """

    def __init__(self, config: dict[str, Any], dialect: str | None):
        super().__init__()
        self._can_fix = True
        self._errors: set[str] = set()
        self._fixed: str | None = None
        self._config = config
        self._dynamic_tables: dict[str, set[str]] = {}
        self._dialect = dialect or ""
        self._risk: list[float] = []
        self.current_in_scope_tables: set[str] = set()

    @property
    def can_fix(self) -> bool:
        """
        Indicates if the query can be automatically fixed/rewritten.

        Returns:
            bool: True if the query can be fixed, False otherwise.
        """
        return self._can_fix

    def add_error(self, error: str, can_fix: bool, risk: float) -> None:
        """
        Adds a verification error and updates query fixability and risk scores.

        Args:
            error (str): The error message to record.
            can_fix (bool): Whether the issue can be automatically resolved/rewritten.
            risk (float): The risk score associated with this error (0.0 to 1.0).
        """
        self._errors.add(error)
        if not can_fix:
            self._can_fix = False
        self._risk.append(risk)

    @property
    def errors(self) -> set[str]:
        """
        Gets the set of recorded verification error messages.

        Returns:
            set[str]: The verification error messages.
        """
        return self._errors

    @property
    def fixed(self) -> str | None:
        """
        Gets the automatically fixed SQL query string, if any.

        Returns:
            str | None: The modified SQL query, or None if no fix was applied.
        """
        return self._fixed

    @fixed.setter
    def fixed(self, value: str | None) -> None:
        """
        Sets the automatically fixed SQL query string.

        Args:
            value (str | None): The modified SQL query string.
        """
        self._fixed = value

    @property
    def config(self) -> dict[str, Any]:
        """
        Gets the restriction configuration dictionary.

        Returns:
            dict[str, Any]: The configuration dict.
        """
        return self._config

    @property
    def dynamic_tables(self) -> dict[str, set[str]]:
        """
        Gets the mapping of registered dynamic tables (such as subqueries and CTEs) to their columns.

        Returns:
            dict[str, set[str]]: A dict mapping table aliases to their column sets.
        """
        return self._dynamic_tables

    @property
    def dialect(self) -> str:
        """
        Gets the SQL dialect used for parsing.

        Returns:
            str: The SQL dialect name.
        """
        return self._dialect

    @property
    def risk(self) -> float:
        """
        Calculates the aggregate risk score from all recorded errors.

        Returns:
            float: The aggregate risk score (from 0.0 to 1.0).
        """
        return sum(self._risk) / len(self._risk) if len(self._risk) > 0 else 0
