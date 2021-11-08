import os
import sys

import click

from sqly.database import Database
from sqly.migration import Migration


@click.group()
def main():
    pass


@main.command()
@click.option(
    '--name', required=False, help="A couple words describing the Migration's purpose"
)
@click.argument('app')
@click.argument('other_apps', nargs=-1)
def migration(app, other_apps, name):
    """
    Create a Migration in APP (importable python module) incorporating dependencies from
    OTHER_APPS
    """
    migration = Migration.create(app, *other_apps, name=name)
    migration.save()
    print(f"Created migration: {migration.key}")
    print("    depends:\n      -", '\n      - '.join(migration.depends or '[]'))


@main.command()
@click.argument('apps', nargs=-1)
def migrations(apps, include_depends=False):
    """
    List the Migrations in APPS
    """
    for app in apps:
        print(
            '\n'.join(
                m.key for m in Migration.app_migrations(app, include_depends=False)
            )
        )


@main.command()
@click.argument('app')
@click.argument('migration_id')
@click.option(
    '-d',
    '--database-url',
    required=False,
    help="Datebase to migrate; default = env $DATABASE_URL",
)
def migrate(app, migration_id, database_url=None):
    """
    Migrate --database | env $DATABASE_URL for the given APP to MIGRATION_ID.
    """
    app_migrations = Migration.app_migrations(app, include_depends=False)
    m_id = migration_id.split('_')[0]  # remove the Migration.name if included

    migration = next(
        filter(lambda migration: migration.id == m_id, app_migrations), None
    )
    if migration is None:
        print(f'Migration not found in {app}:{migration_id}', file=sys.stderr)
        sys.exit(1)

    database_url = database_url or os.getenv('DATABASE_URL')
    if not database_url:
        print('--database-url or env $DATABASE_URL must be set', file=sys.stderr)
        sys.exit(1)

    dialect = Database.connection_string_dialect(database_url)
    database = Database(connection_string=database_url, dialect=dialect)

    Migration.migrate(database, migration)


if __name__ == '__main__':
    main()
