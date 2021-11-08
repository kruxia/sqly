import logging
import sys

import click

from sqly.migration import Migration

logger = logging.getLogger('sqly')


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
@click.argument('app')
@click.argument('migration_id')
def migrate(app, migration_id):
    app_migrations = Migration.app_migrations(app, include_depends=False)
    # remove the Migration.name if included
    m_id = migration_id.split('_')[0]
    migration = next(
        filter(lambda migration: migration.id == m_id, app_migrations), None
    )
    if migration is None:
        print(f'Migration not found in {app}:{migration_id}', file=sys.stderr)
        sys.exit(1)
    Migration.migrate(None, migration)


if __name__ == '__main__':
    main()
