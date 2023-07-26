"""
Definitions for different dialects of SQL databases. Each
[Dialect](./#sqly.dialect.Dialect):

* is named for its database adaptor (driver module).
* defines an [ParamFormat](./#sqly.dialect.ParamFormat) that the Dialect uses to
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
    >>> dialect.param_format
    <ParamFormat.PYFORMAT: '%(field)s'>
    >>> dialect.param_format.is_keyed
    True
    >>> dialect.adaptor_name
    'psycopg'
    >>> dialect.adaptor() # doctest:+ELLIPSIS
    <module 'psycopg' from '.../psycopg/__init__.py'>
"""
from enum import Enum
from importlib import import_module
from typing import Any


class ParamFormat(Enum):
    """
    The parameter format for a given database dialect.
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
        """If true, this `ParamFormat` uses keyword parameters."""
        return self in {self.NAMED, self.PYFORMAT}

    @property
    def is_positional(self) -> bool:
        """If true, this `ParamFormat` uses positional parameters."""
        return not self.is_keyed


class Dialect(Enum):
    """
    Each Dialect is named for the adaptor (driver) interface that it uses and
    encapsulates the options that adaptor uses for such things as query parameter
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
    def param_format(self) -> ParamFormat:
        """The [ParamFormat](./#sqly.dialect.ParamFormat) for this Dialect."""
        return {
            "psycopg": ParamFormat.PYFORMAT,
            "sqlite": ParamFormat.QMARK,
            # --
            # "mysql": ParamFormat.FORMAT,
            # "pyodbc": ParamFormat.QMARK,
            # # --
            # "asyncpg": ParamFormat.NUMBERED,
            # "databases": ParamFormat.NAMED,
            # "sqlalchemy": ParamFormat.PYFORMAT,
            # "psycopg2": ParamFormat.PYFORMAT,
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
