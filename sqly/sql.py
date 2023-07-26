import json
import re
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Optional

from .dialect import Dialect, ParamFormat
from .lib import walk


@dataclass
class SQL:
    """
    Render and execute SQL queries with a given database dialect. All queries are
    rendered according to the requirements of that dialect.

    * Create queries using the ergonomic "named" parameter format (:key).
    * `render()` the query to the parameter format native to the current database
      Dialect.
    * Use the `execute()` and `select()` query methods to `render()` and execute queries
      with "named" parameters on the given database connection.

    Arguments:
        dialect (Dialect): The SQL [dialect](sqly.dialect.md) used by this database.

    **Methods:**

    * [render()](./#sqly.sql.SQL.render): Render a query and accompanying data to
      database Dialect-native form.
    * [execute()](./#sqly.sql.SQL.execute): Execute a query on the given connection.
    * [select()](./#sqly.sql.SQL.select): Execute a query and select the results as
        record objects.

    Examples:
        Initialize the SQL and Connection instances:

        >>> import sqlite3
        >>> from sqly import SQL
        >>> connection = sqlite3.connect(":memory:")
        >>> sql = SQL(dialect="sqlite")

        Create a table to make queries with:

        >>> cursor = sql.execute(connection,
        ...     "CREATE TABLE widgets (id int, sku varchar)")

        Insert a widget:

        >>> widget = {"id": 1, "sku": "COG-01"}
        >>> cursor = sql.execute(cursor,  # <-- cursor can be re-used
        ...     "INSERT INTO widgets VALUES (:id, :sku)", widget)
        >>> connection.commit()

        Select matching widgets:

        >>> records = sql.select(connection,
        ...     "SELECT * FROM widgets WHERE sku like :sku", {"sku": "COG-%"})
        >>> for record in records: print(record)
        {'id': 1, 'sku': 'COG-01'}

    """

    dialect: Dialect

    def __post_init__(self):
        if not isinstance(self.dialect, Dialect):
            self.dialect = Dialect(self.dialect)

    def render(self, query, data=None):
        """
        Render a query string and its parameters for this SQL dialect.

        Arguments:
            query (str | Iterator): a string or iterator of strings.
            data (Mapping): a keyword dict used to render the query parameters.

        Returns:
            (str): the rendered query string.
            (tuple | dict): depends on the param format:

                - positional param formats (QMARK, NUMBERED) return a tuple of values
                - named param formats (NAMED, PYFORMAT) return a dict
        """
        # ordered list of fields for positional outputs (closure for replace_parameter)
        fields = []

        def replace_parameter(match):
            field = match.group(1)

            # Build the ordered fields list
            if self.dialect.param_format.is_positional or field not in fields:
                fields.append(field)

            # Return the field formatted for the param format type
            if self.dialect.param_format == ParamFormat.NAMED:
                return f":{field}"
            elif self.dialect.param_format == ParamFormat.PYFORMAT:
                return f"%({field})s"
            elif self.dialect.param_format == ParamFormat.QMARK:
                return "?"
            elif self.dialect.param_format == ParamFormat.NUMBERED:
                return f"${len(fields)}"
            else:  # self.dialect.param_format == ParamFormat.FORMAT:
                return "%s"

        # 1. Convert query to a string
        if isinstance(query, str):
            query_str = str(query)
        elif hasattr(query, "__iter__"):
            query_str = "\n".join(str(q) for q in walk(query))
        else:
            raise ValueError(f"Query has unsupported type: {type(query)}")

        # 2. Escape string parameters in the PYFORMAT param format
        if self.dialect.param_format == ParamFormat.PYFORMAT:
            # any % must be intended as literal and must be doubled
            query_str = query_str.replace("%", "%%")

        # 3. Replace the parameter with its dialect-specific representation
        pattern = r"(?<!\\):(\w+)\b"  # colon + word not preceded by a backslash
        query_str = re.sub(pattern, replace_parameter, query_str).strip()

        # 4. Un-escape remaining escaped colon params
        if self.dialect.param_format == ParamFormat.NAMED:
            # replace \:word with :word because the colon-escape is no longer needed.
            query_str = re.sub(r"\\:(\w+)\b", r":\1", query_str)

        # 5. Build the parameter_values dict or list for use with the query
        if self.dialect.param_format.is_positional:
            # parameter_values is a list of values
            parameter_values = [
                json.dumps(val) if isinstance(val, dict) else val
                for val in [data[field] for field in fields]
            ]
        else:
            # parameter_values is a dict of key:value fields
            parameter_values = {
                key: json.dumps(val) if isinstance(val, (dict, list, tuple)) else val
                for key, val in {field: data[field] for field in fields}.items()
            }

        # 6. Return a tuple formatted for this Dialect
        # if self.dialect == Dialect.ASYNCPG:
        # asyncpg expects the parameters in a tuple following the query string.
        # return tuple([query_str] + parameter_values)
        # else:
        # other dialects expect the parameters in the second tuple item.
        return (query_str, parameter_values)

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
            cursor = connection.execute(*self.render(query, data))
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
