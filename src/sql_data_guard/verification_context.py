from typing import Set, Dict, List, Optional

from .column_masking import build_mask_lookup


class VerificationContext:
    """
    Context for verifying SQL queries against a given configuration.

    Attributes:
        _can_fix (bool): Indicates if the query can be fixed.
        _errors (List[str]): Ordered, de-duplicated list of errors found during verification.
        _fixed (Optional[str]): The fixed query if modifications were made.
        _config (dict): The configuration used for verification.
        _dynamic_tables (Dict[str, Set[str]]): Dynamic tables (sub selects, WITH clauses) mapped
            to the column names they expose.
        _dynamic_columns (Set[str]): Columns exposed by dynamic sources that have no usable alias
            (e.g. un-aliased FROM sub-queries), accessible un-prefixed by the outer query.
        _dialect (str): The SQL dialect to use for parsing.
        _risk (List[float]): Per-finding risk weights, averaged to produce the final risk score.
    """

    def __init__(self, config: dict, dialect: str):
        super().__init__()
        self._can_fix = True
        # Ordered + de-duplicated so the API response is deterministic (a set is not).
        self._errors: List[str] = []
        self._fixed = None
        self._config = config
        self._dynamic_tables: Dict[str, Set[str]] = {}
        self._dynamic_columns: Set[str] = set()
        self._dialect = dialect
        self._risk: List[float] = []
        # {table_name: {column_name: mask_spec}}
        self._column_masks: Dict[str, Dict[str, dict]] = build_mask_lookup(config)

    @property
    def can_fix(self) -> bool:
        return self._can_fix

    def add_error(self, error: str, can_fix: bool, risk: float):
        if error not in self._errors:
            self._errors.append(error)
        if not can_fix:
            self._can_fix = False
        self._risk.append(risk)

    @property
    def errors(self) -> List[str]:
        return self._errors

    @property
    def fixed(self) -> Optional[str]:
        return self._fixed

    @fixed.setter
    def fixed(self, value: Optional[str]):
        self._fixed = value

    @property
    def config(self) -> dict:
        return self._config

    @property
    def dynamic_tables(self) -> Dict[str, Set[str]]:
        return self._dynamic_tables

    @property
    def dynamic_columns(self) -> Set[str]:
        return self._dynamic_columns

    @property
    def dialect(self) -> str:
        return self._dialect

    @property
    def column_masks(self) -> Dict[str, Dict[str, dict]]:
        return self._column_masks

    @property
    def risk(self) -> float:
        return sum(self._risk) / len(self._risk) if len(self._risk) > 0 else 0
