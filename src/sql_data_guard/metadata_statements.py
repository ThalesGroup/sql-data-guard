"""
Metadata statements (SHOW / DESCRIBE), allowed when the configuration sets `allow_metadata`.

Only a fixed set of forms is recognized; anything else is rejected. DESCRIBE is read from
sqlglot's AST (it parses the same way in every dialect). SHOW is not: most dialects keep it as an
unparsed command, and the tokenizer swallows everything after SHOW into one string token. So a
SHOW statement is matched against the tokens of the text after SHOW, tokenized with the
statement's dialect (quoting and comments are handled as in queries).

The statement is only classified here; whether its target is allowed is decided by the
verification (sql_data_guard._verify_metadata), with the same rules as table references.
"""

import re
from dataclasses import dataclass
from typing import List, Optional, Union

import sqlglot
import sqlglot.expressions as expr
from sqlglot.tokens import Token, TokenType

TABLE_FORMS = ("DESCRIBE", "SHOW COLUMNS", "SHOW CREATE TABLE", "SHOW CREATE VIEW",
               "SHOW PARTITIONS", "SHOW TBLPROPERTIES")


@dataclass
class MetadataStatement:
    """A recognized metadata statement and what it is about."""

    form: str  # e.g. "SHOW TABLES"
    table: Optional[expr.Table] = None  # forms about one table
    database: Optional[str] = None  # SHOW TABLES/VIEWS FROM db, DESCRIBE DATABASE db
    catalog: Optional[str] = None  # SHOW SCHEMAS FROM catalog, SHOW TABLES FROM catalog.db


# A recognized statement, or the label of an unsupported one (for the error message).
Classified = Union[MetadataStatement, str]

_NAME_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_STRUCTURAL = {"FROM", "IN", "LIKE"}


def starts_with_show(sql: str, dialect: Optional[str]) -> bool:
    try:
        tokens = _tokens(sql, dialect)
    except sqlglot.errors.SqlglotError:
        return False  # reported by the parser as usual
    return bool(tokens) and tokens[0].token_type == TokenType.SHOW


def classify_show(sql: str, dialect: Optional[str]) -> Classified:
    """Classifies a statement that starts with SHOW (see starts_with_show)."""
    show = _tokens(sql, dialect)[0]
    rest_sql = sql[show.end + 1:]
    try:
        rest = _tokens(rest_sql, dialect)
    except sqlglot.errors.SqlglotError:
        return "SHOW"
    return _Matcher(rest, rest_sql, dialect).show()


def classify_describe(parsed: expr.Describe) -> Classified:
    """Classifies a parsed DESCRIBE statement."""
    kind = str(parsed.args.get("kind") or "").upper()
    style = str(parsed.args.get("style") or "").upper()
    label = " ".join(p for p in ("DESCRIBE", kind, style) if p)
    extra = [k for k, v in parsed.args.items() if v and k not in ("this", "kind", "style")]
    target = parsed.this
    if extra or not isinstance(target, expr.Table) or not _plain_table(target):
        return label
    if kind in ("DATABASE", "SCHEMA") and not style:
        if target.db:
            return label
        return MetadataStatement("DESCRIBE DATABASE", database=target.name)
    if kind in ("", "TABLE", "VIEW") and style in ("", "EXTENDED", "FORMATTED"):
        return MetadataStatement("DESCRIBE", table=target)
    return label


def _tokens(sql: str, dialect: Optional[str]) -> List[Token]:
    tokens = sqlglot.Dialect.get_or_raise(dialect).tokenize(sql)
    return [t for t in tokens if t.token_type.name != "HIVE_TOKEN_STREAM"]


def _plain_table(table: expr.Table) -> bool:
    """A table name and nothing else (no alias, partition, time travel, sample...)."""
    extra = [k for k, v in table.args.items() if v and k not in ("this", "db", "catalog")]
    return not extra and isinstance(table.this, expr.Identifier)


class _Matcher:
    """A strict matcher over the tokens after SHOW; any token left over is a rejection."""

    def __init__(self, tokens: List[Token], sql: str, dialect: Optional[str]):
        self.tokens = tokens
        self.sql = sql
        self.dialect = dialect
        self.pos = 0

    # ------------------------------------------------------------------ grammar
    def show(self) -> Classified:
        first = self._peek_word()
        label = f"SHOW {first}" if first else "SHOW"
        try:
            result = self._show(first)
        except _NoMatch:
            return label
        if result is None or not self._at_end():
            return label
        return result

    def _show(self, first: Optional[str]) -> Optional[MetadataStatement]:
        if first == "FULL":
            self.pos += 1
            first = self._peek_word()
            if first != "TABLES":
                return None
        if first in ("TABLES", "VIEWS"):
            self.pos += 1
            stmt = MetadataStatement(f"SHOW {first}")
            if self._accept("FROM", "IN"):
                name = self._name(max_parts=2)
                stmt.database, stmt.catalog = name.name, name.db or None
            self._like()
            return stmt
        if first in ("SCHEMAS", "DATABASES"):
            self.pos += 1
            stmt = MetadataStatement(f"SHOW {first}")
            if self._accept("FROM", "IN"):
                self._accept("DATABASE")  # Snowflake: SHOW SCHEMAS IN DATABASE db
                stmt.catalog = self._name(max_parts=1).name
            self._like()
            return stmt
        if first == "CATALOGS":
            self.pos += 1
            self._like()
            return MetadataStatement("SHOW CATALOGS")
        if first == "COLUMNS":
            self.pos += 1
            self._expect("FROM", "IN")
            table = self._name(max_parts=3)
            if self._accept("FROM", "IN"):  # MySQL: SHOW COLUMNS FROM t FROM db
                if table.db:
                    return None
                table.set("db", self._name(max_parts=1).this)
            self._like()
            return MetadataStatement("SHOW COLUMNS", table=table)
        if first == "CREATE":
            self.pos += 1
            kind = self._peek_word()
            if kind not in ("TABLE", "VIEW"):
                return None
            self.pos += 1
            return MetadataStatement(f"SHOW CREATE {kind}", table=self._name(max_parts=3))
        if first in ("PARTITIONS", "TBLPROPERTIES"):
            self.pos += 1
            return MetadataStatement(f"SHOW {first}", table=self._name(max_parts=3))
        return None

    # ------------------------------------------------------------------ helpers
    def _peek(self) -> Optional[Token]:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _peek_word(self) -> Optional[str]:
        """The next token as an upper case keyword, or None (a quoted name or a literal is not one)."""
        token = self._peek()
        if token is None or token.token_type in (TokenType.IDENTIFIER, TokenType.STRING):
            return None
        return token.text.upper() if _NAME_PART.match(token.text) else None

    def _accept(self, *words: str) -> bool:
        if self._peek_word() in words:
            self.pos += 1
            return True
        return False

    def _expect(self, *words: str):
        if not self._accept(*words):
            raise _NoMatch()

    def _like(self):
        if self._accept("LIKE"):
            token = self._peek()
            if token is None or token.token_type != TokenType.STRING:
                raise _NoMatch()
            self.pos += 1

    def _name_part(self) -> bool:
        token = self._peek()
        if token is None:
            return False
        if token.token_type == TokenType.IDENTIFIER:
            return True
        word = self._peek_word()
        return word is not None and word not in _STRUCTURAL

    def _name(self, max_parts: int) -> expr.Table:
        """A dotted name, parsed by sqlglot with the dialect (so quoting works as in queries)."""
        if not self._name_part():
            raise _NoMatch()
        start = self.tokens[self.pos].start
        self.pos += 1
        while (token := self._peek()) is not None and token.token_type == TokenType.DOT:
            self.pos += 1
            if not self._name_part():
                raise _NoMatch()
            self.pos += 1
        end = self.tokens[self.pos - 1].end
        try:
            table = expr.to_table(self.sql[start:end + 1], dialect=self.dialect)
        except sqlglot.errors.SqlglotError:
            raise _NoMatch()
        if table is None or not _plain_table(table) or len(table.parts) > max_parts:
            raise _NoMatch()
        return table

    def _at_end(self) -> bool:
        token = self._peek()
        if token is not None and token.token_type == TokenType.SEMICOLON:
            self.pos += 1
        return self._peek() is None


class _NoMatch(Exception):
    pass
