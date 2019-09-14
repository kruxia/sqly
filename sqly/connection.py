import json
from importlib import import_module

from sqly import lib

from .dialects import Dialects


def get_connection(database_settings, json_encoder=None, json_decoder=None):
    """get a connection for the given database_settings"""
    dialect = database_settings['dialect']
    if dialect == Dialects.SQLITE:
        adaptor = import_module('sqlite3')
    elif dialect == Dialects.PSYCOPG2:
        adaptor = import_module('psycopg2')
    elif dialect == Dialects.ASYNCPG:
        adaptor = import_module('asyncpg')
    else:
        raise ValueError('Unsupported dialect: %r' % dialect)

    conn = lib.run(adaptor.connect(**database_settings['connection']))

    if dialect == Dialects.ASYNCPG:
        lib.run(
            conn.set_type_codec(
                'json',
                encoder=json_encoder or json.dumps,
                decoder=json_decoder or json.loads,
                schema='pg_catalog',
            )
        )

    return conn
