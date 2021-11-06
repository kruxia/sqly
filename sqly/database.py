
from pydantic import BaseModel, Field, validator
from sqly.dialect import Dialect
from sqly.lib import run


class Database(BaseModel):
    connection_string: str = ':memory:'
    dialect: Dialect = Dialect.SQLITE

    def connect(self):
        mod = self.dialect.load_adaptor()
        if self.dialect.adaptor == 'sqlalchemy':
            connection = mod.create_engine(self.connection_string).connect()
        else:
            connection = run(mod.connect(self.connection_string))
        return connection
