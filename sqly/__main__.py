"""
The `sqly.__main__` module provides the `sqly` command line command. This is the code
reference documentation; see the [CLI Usage](../cli.md) document for usage information.
"""
# import json
import os
import sys

import click
import networkx as nx

from .dialect import Dialect
from .migration import Migration


@click.group()
def sqly():  # pragma: no cover
    pass


@sqly.command()
@click.option(
    "-n",
    "--name",
    required=False,
    help="A couple words describing the Migration's purpose",
)
@click.argument("app")
@click.argument("other_apps", nargs=-1)
def migration(app, other_apps, name):
    """
    Create a Migration in APP (importable python module) incorporating dependencies from
    OTHER_APPS
    """
    migration = Migration.create(app, *other_apps, name=name)
    migration.save(exclude={"applied"})
    print(f"Created migration: {migration.key}")
    print("    depends:\n      -", "\n      - ".join(migration.depends or "[]"))


@sqly.command()
@click.argument("apps", nargs=-1)
@click.option("-i", "--include-depends", is_flag=True, help="Include dependencies")
def migrations(apps, include_depends=False):
    """
    List the Migrations in APPS
    """
    for app in apps:
        app_migrations = Migration.app_migrations(app, include_depends=include_depends)
        graph = Migration.graph(Migration.app_migrations(app, include_depends=True))
        for key in nx.lexicographical_topological_sort(graph):
            if key in app_migrations:
                print(key)
                migration = app_migrations[key]
                if migration.depends and include_depends:
                    print("\t=> " + "\n\t=> ".join(migration.depends))


@sqly.command()
@click.argument("migration_key")
@click.option(
    "-u",
    "--database-url",
    required=False,
    help="Datebase to migrate; default = env $DATABASE_URL",
)
@click.option("-d", "--dialect", required=False)
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

    dialect = Dialect(dialect)
    adaptor = dialect.adaptor()
    # if dialect == Dialect.MYSQL:
    #     conn_info = json.loads(database_url)
    #     connection = adaptor.connect(**conn_info)
    # else:
    connection = adaptor.connect(database_url)

    migration = Migration.key_load(migration_key)
    Migration.migrate(connection, dialect, migration, dryrun=dryrun)


if __name__ == "__main__":  # pragma: no cover
    sqly()
