import asyncio
import json
from importlib import import_module

from .dialects import Dialects


def get_settings(mod_name, settings_mod_name=None):
    if not settings_mod_name:
        settings_mod_name = f"{mod_name}.settings"
    return import_module(settings_mod_name)


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

    conn = connection_run(adaptor.connect(**database_settings['connection']))

    if dialect == Dialects.ASYNCPG:
        connection_run(
            conn.set_type_codec(
                'json',
                encoder=json_encoder or json.dumps,
                decoder=json_decoder or json.loads,
                schema='pg_catalog',
            )
        )

    return conn


def connection_run(result):
    if asyncio.iscoroutine(result):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(result)
    return result