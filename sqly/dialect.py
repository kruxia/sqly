from enum import Enum
from importlib import import_module


class OutputFormat(Enum):
    # -- positional --
    QMARK = "?"
    FORMAT = "%s"
    NUMBERED = "$i"

    # -- keyed --
    NAMED = ":field"
    PYFORMAT = "%(field)s"

    @property
    def is_keyed(self):
        return self in {self.NAMED, self.PYFORMAT}

    @property
    def is_positional(self):
        return not self.is_keyed


class Dialect(Enum):
    # the value of each Dialect represents the syntax that it uses for query parameters
    PSYCOPG = "psycopg"
    # --
    ASYNCPG = "asyncpg"
    DATABASES = "databases"
    SQLALCHEMY = "sqlalchemy"
    SQLITE = "sqlite"
    # --
    # PSYCOPG2 = "psycopg2"
    # PYODBC = "pyodbc"

    @property
    def output_format(self):
        return {
            self.PSYCOPG: OutputFormat.PYFORMAT,
            # --
            self.ASYNCPG: OutputFormat.NUMBERED,
            self.DATABASES: OutputFormat.NAMED,
            self.SQLALCHEMY: OutputFormat.PYFORMAT,
            self.SQLITE: OutputFormat.QMARK,
            # --
            # self.PSYCOPG2: OutputFormat.PYFORMAT,
            # self.PYODBC: OutputFormat.QMARK,
        }[self]

    @property
    def adaptor(self):
        return {
            self.PSYCOPG: "psycopg",
            # --
            self.ASYNCPG: "asyncpg",
            self.DATABASES: "databases",
            self.SQLALCHEMY: "sqlalchemy",
            self.SQLITE: "sqlite3",
            # --
            # self.PSYCOPG2: "psycopg2",
            # self.PYODBC: "pyodbc",
        }[self]

    def load_adaptor(self):
        return import_module(self.adaptor)
