from dataclasses import dataclass

from sqly.dialects import DEFAULT_DIALECT
from sqly.query import Query


@dataclass
class SQL:
    """interface class: Enable creating queries with a given dialect in the whole app.
    """

    dialect: str = DEFAULT_DIALECT

    def query(self, *args, dialect=None, **kwargs):
        if not dialect:
            dialect = self.dialect
        return Query(*args, dialect=dialect, **kwargs)

    def select(self):
        return Query(['select {fields} from {table} {where}'], dialect=self.dialect)

    def insert(self):
        return Query(
            [
                'insert into {table} ({fields}) values ({params})',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        )

    def update(self):
        return Query(
            [
                'update {table} set {assigns} {where}',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        )

    def delete(self):
        return Query(
            [
                'delete from {table} {where}',
                'returning *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        )

    def upsert(self):
        return Query(
            [
                'INSERT INTO {table} ({fields}) VALUES ({params})',
                'ON CONFLICT ({keys}) DO UPDATE SET {assigns_excluded}',
                'RETURNING *' if self.dialect.supports_returning else '',
            ],
            dialect=self.dialect,
        )
