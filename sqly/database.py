from pydantic import BaseModel

from .dialect import Dialect
from .lib import run_sync


class Database(BaseModel):
    connection_string: str = ':memory:'
    dialect: Dialect = Dialect.SQLITE

    def connect(self):
        mod = self.dialect.load_adaptor()
        if self.dialect.adaptor == 'sqlalchemy':
            connection = mod.create_engine(self.connection_string).connect()
        else:
            connection = run_sync(mod.connect(self.connection_string))

        return connection
