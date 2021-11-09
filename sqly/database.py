import logging

from pydantic import BaseModel

from .dialect import Dialect
from .lib import run

log = logging.getLogger('sqly.database')


class Database(BaseModel):
    connection_string: str = ':memory:'
    dialect: Dialect = Dialect.SQLITE

    @classmethod
    def connection_string_dialect(cls, connection_string):
        dialect = None
        if connection_string == ':memory:':
            dialect = Dialect.SQLITE

        if connection_string.startswith('postgres'):
            for dialect in [Dialect.ASYNCPG, Dialect.PSYCOPG2, Dialect.POSTGRES]:
                try:
                    dialect.load_adaptor()
                    break
                except Exception:
                    pass

        if connection_string.startswith('mysql'):
            dialect = Dialect.MYSQL

        if not dialect:
            dialect = Dialect.PYODBC

        return dialect

    def connect(self):
        mod = self.dialect.load_adaptor()
        if self.dialect.adaptor == 'sqlalchemy':
            connection = mod.create_engine(self.connection_string).connect()
        else:
            connection = run(mod.connect(self.connection_string))
        return connection
