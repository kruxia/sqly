from pydantic import BaseModel
from .dialect import Dialect
from .lib import walk


class SQL(BaseModel):
    """
    Interface class: Enable creating and rendering queries with a given dialect. All
    queries are rendered with that dialect.
    """

    dialect: Dialect

    def render(self, query, data=None):
        if isinstance(query, str):
            query_str = query
        elif hasattr(query, '__iter__'):
            query_str = '\n'.join(walk(query))
        else:
            raise ValueError(f"Query has unsupported type: {type(query)}")

        return self.dialect.render(query_str, data or {})
