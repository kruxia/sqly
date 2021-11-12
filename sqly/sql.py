from pydantic import BaseModel

from .dialect import DEFAULT_DIALECT, Dialect
from .query import Query


class SQL(BaseModel):
    """
    Interface class: Enable creating and rendering queries with a given dialect. All
    queries are rendered with that dialect.
    """

    dialect: Dialect = DEFAULT_DIALECT

    def query(self, query_text, data=None):
        data = data or {}
        return Query(query=query_text, dialect=self.dialect).render(data)

    def select(self, tablename, data=None, filter_keys=None):
        data = data or {}
        filter_data = {k: data[k] for k in filter_keys or data}
        return Query(
            query=[
                f'select {Query.fields(data)}',
                f'from {tablename}',
                f'where {Query.filters(filter_data)}',
            ],
            dialect=self.dialect,
        ).render(data)

    def delete(self, tablename, data=None, filter_keys=None):
        data = data or {}
        filter_data = {k: data[k] for k in filter_keys or data}
        return Query(
            query=[
                f'delete from {tablename}',
                f'where {Query.filters(filter_data)}' if filter_data else '',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        ).render(filter_data)

    def insert(self, tablename, data):
        return Query(
            query=[
                f'insert into {tablename}',
                f'({Query.fields(data)}) values ({Query.params(data)})',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        ).render(data)

    def update(self, tablename, data, filter_keys=None):
        filter_data = {k: data[k] for k in filter_keys or data}
        return Query(
            query=[
                f'update {tablename} set {Query.assigns(data)}',
                f'where {Query.filters(filter_data)}',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        ).render(data)

    def upsert(self, tablename, data, key_fields):
        return Query(
            query=[
                f'INSERT INTO {tablename} ({Query.fields(data)})',
                f'VALUES ({Query.params(data)})',
                f'ON CONFLICT ({key_fields}) DO UPDATE',
                f'SET {Query.assigns(data, excludes=key_fields)}',
                'RETURNING *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        ).render(data)
