import json
import os
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from glob import glob
from importlib import import_module
from pathlib import Path

import networkx as nx
import yaml

from . import queries
from .query import Q
from .sql import SQL

# enable repeatable UUID-as-hash for migration keys by using the repo as the namespace.
SQLY_UUID_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_URL, "https://github.com/kruxia/sqly")


def app_migrations_path(app):
    """
    For a given app name, get the path to its migrations directory.
    """
    mod = import_module(app)
    mod_filepath = Path(next(iter(mod.__path__)))
    return mod_filepath / "migrations"


def migration_timestamp():
    """
    An integer with the UTC timestamp to millisecond resolution (17 digits => bigint)
    """
    return int(datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S%f")[:-3])


@dataclass
class Migration:
    app: str
    ts: int = field(default_factory=migration_timestamp)
    name: str = field(default_factory=str)
    depends: list[str] = field(default_factory=list)
    applied: datetime | None = None
    doc: str | None = None
    up: str | None = None
    dn: str | None = None

    def __post_init__(self):
        # replace non-word characters in the name with an underscore
        self.name = re.sub(r"[\W_]+", "_", self.name or "")

        # ensure that depends is a list
        if self.depends:
            if isinstance(self.depends, str):
                self.depends = json.loads(self.depends)
            else:
                self.depends = list(self.depends)
        else:
            self.depends = []

    def __repr__(self):
        return (
            self.__class__.__name__
            + "("
            + ", ".join(
                f"{key}={getattr(self, key)!r}" for key in ["key", "depends", "applied"]
            )
            + ")"
        )

    def __str__(self):
        return self.yaml()

    def __hash__(self):
        return uuid.uuid3(SQLY_UUID_NAMESPACE, self.key).int

    def dict(self, exclude=None, exclude_none=False):
        return {
            key: val
            for key, val in asdict(self).items()
            if key not in (exclude or []) and (exclude_none is False or val is not None)
        }

    @property
    def key(self):
        """
        The key uniquely identifies the migration. Format = "{app}:{id}_{name}"
        """
        return f"{self.app}:{self.ts}_{self.name}"

    @property
    def filename(self):
        return f"{self.ts}_{self.name}.yaml"

    @classmethod
    def load(cls, filepath):
        with open(filepath) as f:
            data = yaml.safe_load(f.read())

        return cls(**data)

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
        migrations = {
            m.key: m
            for m in set(cls.load(filename) for filename in migration_filenames)
        }
        if include_depends is True:
            dependencies = {}
            for migration in migrations.values():
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
        )
        return migration

    @classmethod
    def graph(cls, migrations):
        """
        Given an iterable of Migrations, create a dependency graph of Migrations by key.
        """
        graph = nx.DiGraph()
        dag = {key: migrations[key].depends for key in migrations}
        for migration_key, migration_depends in dag.items():
            graph.add_node(migration_key)
            for depend in migration_depends:
                graph.add_edge(depend, migration_key)

        if not nx.is_directed_acyclic_graph(graph):
            raise nx.HasACycle(dag)

        return nx.transitive_reduction(graph)

    @classmethod
    def database_migrations(cls, connection):
        try:
            cursor = connection.execute("select * from sqly_migrations")
            fields = [d[0] for d in cursor.description]
            records = []
            for row in cursor:
                records.append(dict(zip(fields, row)))
        except Exception as exc:
            print(str(exc))
            connection.rollback()
            records = []

        return {m.key: m for m in set(cls(**record) for record in records)}

    @classmethod
    def migrate(cls, connection, dialect, migration, dryrun=False):
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
        db_migrations = cls.database_migrations(connection)
        migrations = db_migrations | cls.all_migrations(migration.app)
        graph = cls.graph(migrations)

        if migration.key not in db_migrations:
            # apply 'up' migrations for all ancestors and this migration
            subgraph = nx.subgraph(graph, migration.ancestors(graph) | {migration.key})
            for key in nx.lexicographical_topological_sort(subgraph):
                if key not in db_migrations:
                    migrations[key].apply(
                        connection, dialect, direction="up", dryrun=dryrun
                    )
        else:
            # apply 'dn' migrations for all descendants in reverse
            subgraph = nx.subgraph(graph, migration.descendants(graph))
            for key in reversed(list(nx.lexicographical_topological_sort(subgraph))):
                if key in db_migrations:
                    migrations[key].apply(
                        connection, dialect, direction="dn", dryrun=dryrun
                    )

    def depends_migrations(self):
        """
        All migrations that this migration depends on, recursively.
        """
        dependencies = {}
        for depend in self.depends:
            migration = self.key_load(depend)
            dependencies |= {depend: migration} | migration.depends_migrations()
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

    def apply(self, connection, dialect, direction="up", dryrun=False):
        """
        Apply the migration (direction = 'up' or 'dn') to connection database. The
        entire migration script is wrapped in a transaction.
        """
        print(self.key, direction, end=" ... ")

        if dryrun:
            print("DRY RUN")
            return

        connection.execute("begin;")

        sql = getattr(self, direction)
        connection.execute(sql)

        if direction == "up":
            query = self.insert_query(dialect)
        else:
            query = self.delete_query(dialect)

        connection.execute(*query)
        connection.execute("commit;")
        print("OK")

    def insert_query(self, dialect):
        """
        Insert this migration into the sqly_migrations table.
        """
        data = {k: v for k, v in self.dict(exclude_none=True).items()}
        data["depends"] = json.dumps(data.get("depends") or [])
        sql = queries.INSERT("sqly_migrations", data)
        return SQL(dialect=dialect).render(sql, data)

    def delete_query(self, dialect):
        """
        Delete this migration from the sqly_migrations table.
        """
        sql = queries.DELETE(
            "sqly_migrations", [Q.filter(key) for key in ["app", "ts", "name"]]
        )
        return SQL(dialect=dialect).render(sql, self.dict())
