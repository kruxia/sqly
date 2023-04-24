import json
import os
import re
import uuid
from datetime import datetime, timezone
from glob import glob
from importlib import import_module
from pathlib import Path
from typing import List

import networkx as nx
import yaml
from pydantic import BaseModel, Field, validator

from .dialect import Dialect
from .lib import run_sync
from .sql import SQL

# enable repeatable UUID-as-hash for migration keys by using the repo as the namespace.
SQLY_UUID_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_URL, "https://github.com/kruxia/sqly")


def app_migrations_path(app):
    """
    For a given app name, get the path to its migrations directory.
    """
    mod = import_module(app)
    mod_filepath = Path(mod.__file__).parent
    return mod_filepath / "migrations"


def make_migration_timestamp():
    """
    An integer with the UTC timestamp to millisecond resolution (17 digits => bigint)
    """
    return int(datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S%f")[:-3])


class Migration(BaseModel):
    app: str
    ts: int = Field(default_factory=make_migration_timestamp)
    name: str = Field(default_factory=str)
    depends: List[str] = Field(default_factory=list)
    applied: datetime | None = Field(default=None, exclude=True)
    doc: str | None = Field(default=None)
    up: str | None = Field(default=None)
    upsh: str | None = Field(default=None)
    dn: str | None = Field(default=None)
    dnsh: str | None = Field(default=None)

    @validator("name", pre=True, always=True)
    def name_convert(cls, value):
        # replace non-word characters in the name with an underscore
        return re.sub(r"[\W_]+", "_", value)

    @validator("depends", pre=True, always=True)
    def depends_default_empty_list(cls, value):
        if isinstance(value, str):
            return json.loads(value)
        else:
            return value or []

    def __repr__(self):
        return (
            "Migration("
            + ", ".join(
                f"{key}={getattr(self, key)!r}" for key in ["key", "depends", "applied"]
            )
            + ")"
        )

    def __str__(self):
        return self.yaml()

    def __hash__(self):
        return uuid.uuid3(SQLY_UUID_NAMESPACE, self.key).int

    @property
    def key(self):
        """
        The key uniquely identifies the migration. Format = "{app}:{id}_{name}"
        """
        return f"{self.app}:{self.ts}_{self.name or ''}"

    @property
    def filename(self):
        return f'{self.ts}_{self.name or ""}.yaml'

    @classmethod
    def load(cls, filepath):
        with open(filepath) as f:
            data = yaml.safe_load(f.read())

        return cls.parse_obj(data)

    @classmethod
    def key_load(cls, migration_key):
        return cls.load(cls.key_filepath(migration_key))

    @classmethod
    def key_filepath(cls, migration_key):
        app, basename = migration_key.split(":")
        return app_migrations_path(app) / f"{basename}.yaml"

    @classmethod
    def app_migrations(cls, app, include_depends=True):
        """
        For a given module name, get the migrations in that module. If include_depends
        is True (default), also include depends migrations from other apps.
        """
        migration_filenames = glob(str(app_migrations_path(app) / "*.yaml"))
        migrations = set(cls.load(filename) for filename in migration_filenames)
        if include_depends is True:
            dependencies = set()
            for migration in migrations:
                dependencies |= migration.depends_migrations()

            migrations |= dependencies

        return migrations

    @classmethod
    def all_migrations(cls, *apps):
        # always depend on sqly
        migrations = cls.app_migrations("sqly")
        for app in [app for app in apps if app not in ["sqly"]]:
            migrations |= cls.app_migrations(app, include_depends=True)

        return migrations

    @classmethod
    def create(cls, app, *other_apps, name=None):
        """
        Every new migration depends on all the "leaf" nodes in the existing migration
        graph. Leaf nodes are those with out_degree == 0 (no edges pointing out). See:
        <https://networkx.org/documentation/stable/reference/classes/generated/networkx.DiGraph.out_degree.html>.
        For a worked example, see:
        <https://stackoverflow.com/questions/31946253/find-end-nodes-leaf-nodes-in-radial-tree-networkx-graph/31953001>.

        NOTE: The existing migration graph is calculated from the filesystem, not what
        is applied in any database. (Migrations from other branches might currently be
        applied in the database; that is a concern for another time.)
        """
        migrations = cls.all_migrations(app, *other_apps)
        graph = cls.graph(migrations)
        depends = [node for node in graph.nodes() if graph.out_degree(node) == 0]
        migration = cls(
            app=app,
            name=name,
            depends=depends,
            doc=None,
            up=None,
            dn=None,
            upsh=None,
            dnsh=None,
        )
        return migration

    @classmethod
    def graph(cls, migrations):
        """
        Given an iterable of Migrations, create a dependency graph of Migrations by key.
        """
        graph = nx.DiGraph()
        dag = {m.key: m.depends for m in migrations}
        for migration_key, migration_depends in dag.items():
            graph.add_node(migration_key)
            for depend in migration_depends:
                graph.add_edge(depend, migration_key)

        if not nx.is_directed_acyclic_graph(graph):
            raise nx.HasACycle(dag)

        return nx.transitive_reduction(graph)

    @classmethod
    def database_migrations(cls, database, connection=None):
        connection = connection or run_sync(database.connect())
        if database.dialect == Dialect.ASYNCPG:
            select = connection.fetch
        else:
            select = connection.execute

        try:
            rows = run_sync(select("select * from sqly_migrations"))
            if database.dialect == Dialect.ASYNCPG:
                records = rows
            else:
                records = []
                fields = [d[0] for d in rows.description]
                for row in rows:
                    records.append(dict(zip(fields, row)))

            return {m.key: m for m in [cls(**record) for record in records]}

        except Exception as exc:
            print(exc)
            return {}

    @classmethod
    def migrate(cls, database, migration, connection=None, dryrun=False):
        """
        Migrate the database to this migration, either up or down, using the given
        (sqly) Database.

        1. Collate the list of applied migrations in the database with the list of
           migrations available in this application. (Edge case: There are no migrations
           in the database because the sqly_migrations table does not exist.)
        2. Calculate the graph path to reach this migration
            - if this migration has not been applied to the database, then the graph
              path is from the last applied predecessor up to this migration.
            - if this migration has been applied to the database, then the graph path is
              from the last applied successor down to this migration.
        3. Apply the sequence of migrations (either up or down).
        """
        connection = connection or run_sync(database.connect())

        db_migrations = cls.database_migrations(database, connection=connection)
        migrations = db_migrations | {
            m.key: m for m in cls.all_migrations(migration.app)
        }
        graph = cls.graph(migrations.values())

        if migration.key not in db_migrations:
            # apply 'up' migrations for all ancestors and this migration
            subgraph = nx.subgraph(graph, migration.ancestors(graph) | {migration.key})
            for key in nx.lexicographical_topological_sort(subgraph):
                if key not in db_migrations:
                    migrations[key].apply(
                        database, direction="up", connection=connection, dryrun=dryrun
                    )
        else:
            # apply 'dn' migrations for all descendants in reverse
            subgraph = nx.subgraph(graph, migration.descendants(graph))
            for key in reversed(list(nx.lexicographical_topological_sort(subgraph))):
                if key in db_migrations:
                    migrations[key].apply(
                        database, direction="dn", connection=connection, dryrun=dryrun
                    )

    def depends_migrations(self):
        """
        All migrations that this migration depends on, recursively.
        """
        dependencies = set()
        for depend in self.depends:
            depend_migration = self.load(self.key_filepath(depend))
            dependencies |= {depend_migration} | depend_migration.depends_migrations()
        return dependencies

    def ancestors(self, graph):
        return nx.ancestors(graph, self.key)

    def descendants(self, graph):
        return nx.descendants(graph, self.key)

    def yaml(self, **kwargs):
        return yaml.dump(self.dict(**kwargs), default_flow_style=False, sort_keys=False)

    def save(self, **kwargs):
        filepath = app_migrations_path(self.app) / self.filename
        os.makedirs(filepath.parent, exist_ok=True)
        with open(filepath, "wb") as f:
            size = f.write(self.yaml(**kwargs).encode())

        return filepath, size

    def apply(self, database, direction="up", connection=None, dryrun=False):
        """
        Apply the migration (direction = 'up' or 'dn') to connection database. The
        entire migration script is wrapped in a transaction.
        """
        print(self.key, direction, end=" ... ")

        if dryrun:
            print("DRY RUN")
            return

        connection = connection or run_sync(database.connect())
        run_sync(connection.execute("begin;"))

        sql = getattr(self, direction, None)
        if sql:
            # (asyncpg does executescript via regular execute)
            if database.dialect == database.dialect.ASYNCPG:
                run_sync(connection.execute(sql))
            else:
                run_sync(connection.executescript(sql))

        sh = getattr(self, f"{direction}sh", None)
        if sh:
            # run the sh cmd relative to the directory in which the migration is defined
            os.chdir(self.__class__.key_filepath(self.key))
            os.system(sh)

        if direction == "up":
            query = self.insert_query(database)
        else:
            query = self.delete_query(database)

        run_sync(connection.execute(*query))
        run_sync(connection.execute("commit;"))
        print("OK")

    def insert_query(self, database):
        """
        Insert this migration into the sqly_migrations table.
        """
        data = {k: v for k, v in self.dict(exclude_none=True).items()}
        if not isinstance(data.get("depends"), str):
            data["depends"] = json.dumps(data.get("depends") or [])
        keys = [k for k in data.keys()]
        params = [f":{k}" for k in keys]
        sql = f"""
            INSERT INTO sqly_migrations ({','.join(keys)})
            VALUES ({','.join(params)});
            """
        return SQL(database.dialect).render(sql, data)

    def delete_query(self, database):
        sql = """
            DELETE FROM sqly_migrations
            WHERE app=:app
                and ts=:ts
                and name=:name
            """
        return SQL(database.dialect).render(sql, self.dict())
