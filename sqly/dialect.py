"""
Definitions for different dialects of SQL databases. Each
[Dialect](./#sqly.dialect.Dialect):

* is named for its database adaptor (driver module).
* defines an [OutputFormat](./#sqly.dialect.OutputFormat) that the Dialect uses to
  render queries with parameters.
* has an [`adaptor()`](./#sqly.dialect.Dialect.adaptor) method that imports and returns 
  the adaptor itself.

Examples:
    List the supported Dialects:
    >>> for dialect in Dialect.__members__.values(): print(repr(dialect))
    <Dialect.PSYCOPG: 'psycopg'>
    <Dialect.SQLITE: 'sqlite'>

    Interact with one of the Dialects:
    >>> from sqly import Dialect
    >>> dialect = Dialect("psycopg")
    >>> dialect.output_format
    <OutputFormat.PYFORMAT: '%(field)s'>
    >>> dialect.output_format.is_keyed
    True
    >>> dialect.adaptor_name
    'psycopg'
    >>> dialect.adaptor() # doctest:+ELLIPSIS
    <module 'psycopg' from '.../psycopg/__init__.py'>
"""
from enum import Enum
from importlib import import_module
from typing import Any


class OutputFormat(Enum):
    """
    The output format for a given dialect.
    """

    # -- positional --
    QMARK = "?"
    FORMAT = "%s"
    NUMBERED = "$i"

    # -- keyed --
    NAMED = ":field"
    PYFORMAT = "%(field)s"

    @property
    def is_keyed(self) -> bool:
        """If true, this `OutputFormat` uses keyword parameters."""
        return self in {self.NAMED, self.PYFORMAT}

    @property
    def is_positional(self) -> bool:
        """If true, this `OutputFormat` uses positional parameters."""
        return not self.is_keyed


class Dialect(Enum):
    """
    Each Dialect is named for the adaptor (driver) interface that it uses and
    encapsulates the options that interface uses for such things as query parameter
    formatting.
    """

    PSYCOPG = "psycopg"
    SQLITE = "sqlite"
    # --
    # MYSQL = "mysql"
    # PYODBC = "pyodbc"
    # # --
    # ASYNCPG = "asyncpg"
    # DATABASES = "databases"
    # SQLALCHEMY = "sqlalchemy"
    # PSYCOPG2 = "psycopg2"

    @property
    def output_format(self) -> OutputFormat:
        """The [OutputFormat](./#sqly.dialect.OutputFormat) for this Dialect."""
        return {
            "psycopg": OutputFormat.PYFORMAT,
            "sqlite": OutputFormat.QMARK,
            # --
            # "mysql": OutputFormat.FORMAT,
            # "pyodbc": OutputFormat.QMARK,
            # # --
            # "asyncpg": OutputFormat.NUMBERED,
            # "databases": OutputFormat.NAMED,
            # "sqlalchemy": OutputFormat.PYFORMAT,
            # "psycopg2": OutputFormat.PYFORMAT,
        }[self.value]

    @property
    def adaptor_name(self) -> str:
        """The name of the adaptor (driver module) to import for this Dialect."""
        return {
            "psycopg": "psycopg",
            "sqlite": "sqlite3",
            # --
            # "mysql": "MySQLdb",
            # "pyodbc": "pyodbc",
            # # --
            # "asyncpg": "asyncpg",
            # "databases": "databases",
            # "sqlalchemy": "sqlalchemy",
            # "psycopg2": "psycopg2",
        }[self.value]

    def adaptor(self) -> Any:
        """The adaptor (driver module) itself for this Dialect.

        Returns:
            (Any): A database adaptor (driver module).
        """
        return import_module(self.adaptor_name)
