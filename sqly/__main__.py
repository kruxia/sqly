import logging

import click

from sqly import connection, lib, schema

logger = logging.getLogger('sqly')


@click.group()
def main():
    pass


@main.command()
@click.argument('mod_name')
@click.option('-s', '--settings_mod_name', required=False)
@click.option('--loglevel', required=False)
@click.argument('requires', nargs=-1, required=False)
def init(mod_name, settings_mod_name, loglevel, requires):
    settings = lib.get_settings(mod_name, settings_mod_name)
    logging.basicConfig(**lib.get_logging_settings(settings, loglevel))
    migration_name = schema.init_app(mod_name, requires=requires)
    print(f"sqly: initialized app: {mod_name}")
    print(f"sqly: created migration: {mod_name}:{migration_name}")


@main.command()
@click.option('-s', '--settings_mod_name', required=False)
@click.option('--loglevel', required=False)
@click.argument('mod_name')
@click.argument('label', required=False)
def migration(mod_name, label, settings_mod_name, loglevel):
    settings = lib.get_settings(mod_name, settings_mod_name)
    logging.basicConfig(**lib.get_logging_settings(settings, loglevel))
    migration_name = schema.create_migration(mod_name, label=label)
    print(f"sqly: created migration: {mod_name}:{migration_name}")


@main.command()
@click.option('-s', '--settings_mod_name', required=False)
@click.option('--loglevel', required=False)
@click.argument('mod_name')
@click.argument('migration_name', required=False)
def migrate(mod_name, settings_mod_name, loglevel, migration_name=None):
    settings = lib.get_settings(mod_name, settings_mod_name)
    logging.basicConfig(**lib.get_logging_settings(settings, loglevel))
    database_settings = settings.DATABASE
    conn = connection.get_connection(database_settings)
    schema.apply_migrations(conn, mod_name, migration_name)


@main.command()
@click.argument('mod_name')
@click.argument('migration_name')
@click.option('--loglevel', required=False)
@click.option('-s', '--settings_mod_name', required=False)
def reverse(mod_name, migration_name, settings_mod_name, loglevel):
    settings = lib.get_settings(mod_name, settings_mod_name)
    logging.basicConfig(**lib.get_logging_settings(settings, loglevel))
    database_settings = settings.DATABASE
    conn = connection.get_connection(database_settings)
    schema.revert_migrations(conn, mod_name, migration_name)


if __name__ == '__main__':
    main()
