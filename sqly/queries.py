"""
A library of prepared queries (in Python, not the database) for use by other programs.
"""

from sqly import DEFAULT_DIALECT, SQL


def select(dialect=DEFAULT_DIALECT):
    return SQL(['select {fields} from {table} {where}'], dialect=dialect)


def insert(dialect=DEFAULT_DIALECT):
    return SQL(
        [
            'insert into {table} ({fields}) values ({params})',
            'returning *' if dialect.supports_returning else '',
        ],
        dialect=dialect,
    )


def update(dialect=DEFAULT_DIALECT):
    return SQL(
        [
            'update {table} set {assigns} {where}',
            'returning *' if dialect.supports_returning else '',
        ],
        dialect=dialect,
    )


def delete(dialect=DEFAULT_DIALECT):
    return SQL(
        [
            'delete from {table} {where}',
            'returning *' if dialect.supports_returning else '',
        ],
        dialect=dialect,
    )


def upsert(dialect=DEFAULT_DIALECT):
    return SQL(
        [
            'INSERT INTO {table} ({fields}) VALUES ({params})',
            'ON CONFLICT ({keys}) DO UPDATE SET {assigns_excluded}',
            'RETURNING *' if dialect.supports_returning else '',
        ],
        dialect=dialect,
    )
