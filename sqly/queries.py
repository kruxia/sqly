"""
A library of prepared queries (in Python, not the database) for use by other programs.
"""

from sqly.sql import SQL


def select(dialect=None):
    return SQL(['select {fields} from {table} {where}'], dialect=dialect)


def insert(dialect=None):
    return SQL(['insert into {table} ({fields}) values ({params})'], dialect=dialect)


def update(dialect=None):
    return SQL(['update {table} set {assigns} {where}'], dialect=dialect)


def delete(dialect=None):
    return SQL(['delete from {table} {where}'], dialect=dialect)


def upsert(dialect=None):
    return SQL(
        [
            'INSERT INTO {table} ({fields}) VALUES ({params})',
            'ON CONFLICT ({keys}) DO UPDATE SET {assigns_excluded}',
            'RETURNING *',
        ],
        dialect=dialect,
    )
