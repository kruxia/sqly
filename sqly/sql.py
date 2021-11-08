from dataclasses import dataclass

from sqly.dialect import DEFAULT_DIALECT
from sqly.query import Query


@dataclass
class SQL:
    """interface class: Enable creating queries with a given dialect in the whole app."""

    dialect: str = DEFAULT_DIALECT

    def query(self, *args, **kwargs):
        return Query(*args, dialect=self.dialect, **kwargs)

    def select(self):
        return Query(['select {fields} from {table} {where}'], dialect=self.dialect)

    def insert(self, tablename, data):
        return Query(
            [
                f'insert into {tablename}',
                f'({Query.fields(data)}) values ({Query.params(data)})',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        )

    def update(self, tablename, update_data, filter_fields):
        return Query(
            [
                f'update {tablename} set {Query.assigns(update_data)}', 
                f'where {Query.where(filter_fields)}',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        )

    def delete(self, tablename, filter_fields):
        return Query(
            [
                f'delete from {tablename} where {Query.where(filter_fields)}',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        )

    def upsert(self, tablename, data, key_fields):
        return Query(
            [
                f'INSERT INTO {tablename} ({Query.fields(data)})', 
                f'VALUES ({Query.params(data)})',
                f'ON CONFLICT ({key_fields}) DO UPDATE', 
                f'SET {Query.assigns(data, excludes=key_fields)}',
                'RETURNING *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        )
