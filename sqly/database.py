from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Optional

from .dialect import Dialect
from .sql import SQL


@dataclass
class Database:
    dialect: Dialect

    def __post_init__(self):
        """
        Initialize the Database with a SQL instance for its Dialect so that it can
        `.render()` queries for that dialect.
        """
        if isinstance(self.dialect, str):
            self.dialect = Dialect(self.dialect)
        self.sql = SQL(dialect=self.dialect)

    def execute(
        self, connection: Any, sql_query: str | Iterator, data: Optional[Mapping] = None
    ):
        """
        Execute the given query on the connection.
        """
        # if the connection is a cursor, get the underlying connection.
        if hasattr(connection, 'connection'):
            connection = connection.connection
        try:
            cursor = connection.execute(*self.sql.render(query, data))
        except Exception as exc:
            if hasattr(connection, 'connection'):
                connection = connection.connection
            connection.rollback()
            raise exc

        return cursor

    def query(
        self,
        connection: Any,
        sql_query: str | Iterator,
        data: Optional[Mapping] = None,
        Constructor=dict,
    ):
        """
        Execute the given query on the connection, and yield result records.
        """
        cursor = self.execute(connection, sql_query, data)
        fields = [d[0] for d in cursor.description]
        for row in cursor:
            yield Constructor(zip(fields, row))
