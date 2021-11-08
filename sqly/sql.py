from dataclasses import dataclass

from sqly.dialect import DEFAULT_DIALECT
from sqly.query import Query


@dataclass
class SQL:
    """
    Interface class: Enable creating and rendering queries with a given dialect. All
    queries are rendered with that dialect.
    """

    dialect: str = DEFAULT_DIALECT

    def query(self, *args, **data):
        return Query(*args, dialect=self.dialect).render(data)

    def select(self, tablename, filter_keys=None, **data):
        filter_data = {k: data[k] for k in filter_keys or data}
        return Query(
            [
                f'select {Query.fields(data)}',
                f'from {tablename}',
                f'where {Query.where(filter_data)}',
            ],
            dialect=self.dialect,
        ).render(data)

    def insert(self, tablename, **data):
        return Query(
            [
                f'insert into {tablename}',
                f'({Query.fields(data)}) values ({Query.params(data)})',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        ).render(data)

    def update(self, tablename, filter_keys=None, **data):
        filter_data = {k: data[k] for k in filter_keys or data}
        return Query(
            [
                f'update {tablename} set {Query.assigns(data)}',
                f'where {Query.where(filter_data)}',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        ).render(data)

    def delete(self, tablename, filter_keys=None, **data):
        filter_data = {k: data[k] for k in filter_keys or data}
        return Query(
            [
                f'delete from {tablename} where {Query.where(filter_data)}',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        ).render(filter_data)

    def upsert(self, tablename, key_fields, **data):
        return Query(
            [
                f'INSERT INTO {tablename} ({Query.fields(data)})',
                f'VALUES ({Query.params(data)})',
                f'ON CONFLICT ({key_fields}) DO UPDATE',
                f'SET {Query.assigns(data, excludes=key_fields)}',
                'RETURNING *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        ).render(data)
