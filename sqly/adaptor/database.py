from dataclasses import dataclass

from ..dialect import Dialect
from ..lib import run_sync


@dataclass
class Database:
    """
    The Database class is primarily needed to provide a generalized database connection
    to the Migration engine in order to run migrations.
    """

    connstr: str = ":memory:"
    dialect: Dialect = Dialect.SQLITE

    def __post_init__(self):
        if isinstance(self.dialect, str):
            self.dialect = Dialect(self.dialect)

    def connect(self):
        mod = self.dialect.load_adaptor()
        if self.dialect.adaptor == "sqlalchemy":
            connection = mod.create_engine(self.connstr).connect()
        else:
            connection = run_sync(mod.connect(self.connstr))

        return connection
