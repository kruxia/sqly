from enum import Enum
from importlib import import_module


class OutputFormat(Enum):
    COLON = ":field"
    PERCENT = "%(field)s"
    QMARK = "?"
    NUMBERED = "$i"

    @property
    def is_named(self):
        return "field" in self.value

    @property
    def is_positional(self):
        return not self.is_named


class Dialect(Enum):
    # the value of each Dialect represents the syntax that it uses for query parameters
    ASYNCPG = "asyncpg"
    PSYCOPG2 = "psycopg2"
    PYODBC = "pyodbc"
    SQLALCHEMY = "sqlalchemy"
    SQLITE = "sqlite"
    DATABASES = "databases"

    @property
    def is_async(self):
        return self in [self.ASYNCPG]

    @property
    def supports_returning(self):
        return self in [self.PSYCOPG2, self.ASYNCPG]

    @property
    def output_format(self):
        return {
            self.ASYNCPG: OutputFormat.NUMBERED,
            self.PSYCOPG2: OutputFormat.PERCENT,
            self.PYODBC: OutputFormat.QMARK,
            self.SQLALCHEMY: OutputFormat.PERCENT,
            self.SQLITE: OutputFormat.QMARK,
            self.DATABASES: OutputFormat.COLON,
        }[self]

    @property
    def adaptor(self):
        return {
            self.ASYNCPG: "asyncpg",
            self.PSYCOPG2: "psycopg2",
            self.PYODBC: "pyodbc",
            self.SQLALCHEMY: "sqlalchemy",
            self.SQLITE: "sqlite3",
            self.DATABASES: "databases",
        }[self]

    def load_adaptor(self):
        return import_module(self.adaptor)
