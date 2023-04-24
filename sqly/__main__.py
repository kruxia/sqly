import os
import sys

import click
import networkx as nx

from .database import Database
from .migration import Migration


@click.group()
def main():
    pass


@main.command()
@click.option(
    "-n", "--name", required=False, help="A couple words describing the Migration's purpose"
)
@click.argument("app")
@click.argument("other_apps", nargs=-1)
def migration(app, other_apps, name):
    """
    Create a Migration in APP (importable python module) incorporating dependencies from
    OTHER_APPS
    """
    migration = Migration.create(app, *other_apps, name=name)
    migration.save(exclude={'applied', 'upsh', 'dnsh'})
    print(f"Created migration: {migration.key}")
    print("    depends:\n      -", "\n      - ".join(migration.depends or "[]"))


@main.command()
@click.argument("apps", nargs=-1)
@click.option("-i", "--include-depends", is_flag=True, help="Show dependencies")
def migrations(apps, include_depends=False):
    """
    List the Migrations in APPS
    """
    for app in apps:
        app_migrations = {
            m.key: m
            for m in Migration.app_migrations(app, include_depends=include_depends)
        }
        graph = Migration.graph(Migration.app_migrations(app, include_depends=True))
        for key in nx.lexicographical_topological_sort(graph):
            if key in app_migrations:
                print(key)
                migration = app_migrations[key]
                if migration.depends and include_depends:
                    print("\t=> " + ", ".join(migration.depends))


@main.command()
@click.argument("migration_key")
@click.option(
    "-u",
    "--database-url",
    required=False,
    help="Datebase to migrate; default = env $DATABASE_URL",
)
@click.option(
    "-d",
    "--dialect",
    required=False
)
@click.option(
    "-r",
    "--dryrun",
    is_flag=True,
    help="If present, shows but does not run the migrations",
)
def migrate(migration_key, database_url=None, dialect=None, dryrun=False):
    """
    Migrate database_url to the given MIGRATION_KEY (up or dn).
    """
    database_url = database_url or os.getenv("DATABASE_URL")
    dialect = dialect or os.getenv("DATABASE_DIALECT")
    if not database_url:
        print("--database-url or env $DATABASE_URL must be set", file=sys.stderr)
        sys.exit(1)
    if not dialect:
        print("--dialect or env $DATABASE_DIALECT must be set", file=sys.stderr)
        sys.exit(1)

    database = Database(connection_string=database_url, dialect=dialect)

    migration = Migration.key_load(migration_key)
    Migration.migrate(database, migration, dryrun=dryrun)


if __name__ == "__main__":
    main()
