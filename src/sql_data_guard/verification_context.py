from typing import Set, Dict, List, Optional


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

    def __init__(self, config: dict, dialect: Optional[str]):
        super().__init__()
        self._can_fix = True
        self._errors: Set[str] = set()
        self._fixed: Optional[str] = None
        self._config = config
        self._dynamic_tables: Dict[str, Set[str]] = {}
        self._cte_scopes: List[Set[str]] = []
        self._source_scopes: List[Set[str]] = []
        self._dialect = dialect
        self._risk: List[float] = []

    @property
    def can_fix(self) -> bool:
        return self._can_fix

    def add_error(self, error: str, can_fix: bool, risk: float):
        self._errors.add(error)
        if not can_fix:
            self._can_fix = False
        self._risk.append(risk)

    @property
    def errors(self) -> List[str]:
        """Errors found, sorted so that results are deterministic."""
        return sorted(self._errors)

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
    def cte_names(self) -> Set[str]:
        """
        Names of the CTEs visible from the query being verified (the only dynamic tables usable
        as a table reference): those of its own WITH clause and of enclosing queries.
        """
        return set().union(*self._cte_scopes) if self._cte_scopes else set()

    def push_cte_scope(self):
        """Enter a query: its WITH clause names are visible only until pop_cte_scope()."""
        self._cte_scopes.append(set())

    def pop_cte_scope(self):
        self._cte_scopes.pop()

    def add_cte_name(self, name: str):
        self._cte_scopes[-1].add(name)

    def push_sources(self, names: Set[str]):
        """Enter a query: names of the dynamic tables (CTE/subquery aliases) in its FROM/JOIN."""
        self._source_scopes.append(set(names))

    def pop_sources(self):
        self._source_scopes.pop()

    @property
    def visible_dynamic_tables(self) -> Dict[str, Set[str]]:
        """
        Dynamic tables visible from the query being verified: those in its own FROM/JOIN and
        in enclosing queries (for correlated subqueries). Aliases of unrelated subqueries
        elsewhere in the statement are not visible.
        """
        names = set().union(*self._source_scopes) if self._source_scopes else set()
        return {n: c for n, c in self._dynamic_tables.items() if n in names}

    @property
    def dialect(self) -> Optional[str]:
        return self._dialect

    @property
    def risk(self) -> float:
        return sum(self._risk) / len(self._risk) if len(self._risk) > 0 else 0
