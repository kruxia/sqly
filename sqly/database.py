from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Optional

from .dialect import Dialect
from .sql import SQL


@dataclass
class Database:
    """
    Interface class to make ergonomic queries on a database. Initialize the Database
    instance with a SQL [dialect](../sqly.dialect), then use the query methods on the
    instance to render and execute queries with "named" parameters on the given database
    connection, even if that database's queries use a different parameter style.

    Arguments:
        dialect (Dialect): The SQL [dialect](../sqly.dialect) used by this database.

    **Methods:**

    * [execute()](.#sqly.database.Database.execute): Execute a query on the connection.
    * [select()](.#sqly.database.Database.select): Execute a query and select the results.

    Examples:
        Initialize the Database and Connection instances:
        
        >>> import sqlite3
        >>> from sqly import Database
        >>> connection = sqlite3.connect(":memory:")
        >>> database = Database(dialect="sqlite")
        
        Create a table to make queries with:

        >>> cursor = database.execute(connection,
        ...     "CREATE TABLE widgets (id int, sku varchar)")

        Insert a widget:

        >>> widget = {"id": 1, "sku": "COG-01"}
        >>> cursor = database.execute(cursor,  # cursor can be re-used
        ...     "INSERT INTO widgets VALUES (:id, :sku)", widget)
        >>> connection.commit()

        Select matching widgets:

        >>> records = database.select(connection,
        ...     "SELECT * FROM widgets WHERE sku like :sku", {"sku": "COG-%"})
        >>> for record in records: print(record)
        {'id': 1, 'sku': 'COG-01'}
    """

    dialect: Dialect

    def __post_init__(self):
        if isinstance(self.dialect, str):
            self.dialect = Dialect(self.dialect)
        self.sql = SQL(dialect=self.dialect)

    def execute(
        self, connection: Any, query: str | Iterator, data: Optional[Mapping] = None
    ):
        """
        Execute the given query on the connection and return the connection cursor.

        If the query fails: Rollback the connection and re-raise the exception, as a
        convenience to the user not to leave the connection in an unusable state.

        If `.execute()` is called with a previously-generated cursor, that cursor will
        be reused and the same cursor returned from the method call.

        Parameters:
            connection (Connection | Cursor): A DB-API 2.0 compliant database connection
                or cursor.
            query (str | Iterator): A query that will be rendered with the given data.
            data (Optional[Mapping]): A data mapping that will be rendered as params
                with the query. Optional, but required if the query contains parameters.

        Returns:
            cursor (Cursor): A DB-API 2.0 compliant database cursor.
        """
        try:
            cursor = connection.execute(*self.sql.render(query, data))
        except Exception as exc:
            # If the connection is a cursor, get the underlying connection to rollback,
            # because cursors don't have a rollback method.
            if hasattr(connection, "connection"):
                connection = connection.connection
            connection.rollback()
            raise exc

        return cursor

    def select(
        self,
        connection: Any,
        query: str | Iterator,
        data: Optional[Mapping] = None,
        Constructor=dict,
    ):
        """
        Execute the given query on the connection, and yield result records.

        The `.select()` method is a generator which iterates over a native database 
        cursor. The results of the method can be cast to a list via `list(...)` or can
        be iterated through one at a time.

        If the query fails: Rollback the connection and re-raise the exception, as a
        convenience to the user not to leave the connection in an unusable state.

        Parameters:
            connection (Connection | Cursor): A DB-API 2.0 compliant database connection
                or cursor.
            query (str | Iterator): A query that will be rendered with the given data.
            data (Optional[Mapping]): A data mapping that will be rendered as params
                with the query. Optional, but required if the query contains parameters.
            Constructor (class): A constructor to use to build records from the results.
                The constructor must take the results of `zip(keys, values)` as its 
                argument.
            
        Yields:
            record (Mapping): A mapping object that contains a database record.
        """
        cursor = self.execute(connection, query, data)
        fields = [d[0] for d in cursor.description]
        for row in cursor:
            yield Constructor(zip(fields, row))
