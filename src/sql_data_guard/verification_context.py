

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

    @property
    def can_fix(self) -> bool:
        return self._can_fix

    def add_error(self, error: str, can_fix: bool, risk: float) -> None:
        self._errors.add(error)
        if not can_fix:
            self._can_fix = False
        self._risk.append(risk)

    @property
    def errors(self) -> set[str]:
        return self._errors

    @property
    def fixed(self) -> str | None:
        return self._fixed

    @fixed.setter
    def fixed(self, value: str | None) -> None:
        self._fixed = value

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    @property
    def dynamic_tables(self) -> dict[str, set[str]]:
        return self._dynamic_tables

    @property
    def dialect(self) -> str:
        return self._dialect

    @property
    def risk(self) -> float:
        return sum(self._risk) / len(self._risk) if len(self._risk) > 0 else 0
