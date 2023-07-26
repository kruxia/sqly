"""
Implementation of the sqly migration commands. See the [CLI Usage document](../cli.md)
for more information about usage.
"""
import json
import os
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from glob import glob
from importlib import import_module
from pathlib import Path
from typing import AbstractSet, Any, Dict, ForwardRef, Mapping, Optional

import networkx as nx
import yaml

from . import queries
from .dialect import Dialect
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
    Return an integer with the UTC timestamp to millisecond resolution (17 digits => bigint)
    """
    return int(datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S%f")[:-3])


Migration = ForwardRef("Migration")


@dataclass
class Migration:
    """
    Represents a single migration.

    Arguments:
        app (str): The name of the app (module) that owns the Migration.
        ts (int): (`YYYYmmddHHMMSSfff`) An integer representing the timestamp when the
            migration was created (millisecond resolution).
        name (str): The (optional) name of the migration provides a short description.
        depends (list[str]): A list of migrations (keys) that this migration depends on.
        applied (Optional[datetime]): If the migration has been applied, the datetime.
        doc (Optional[str]): A document string describing the migration.
        up (Optional[str]): SQL implmenting the "up" or "forward" migration.
        dn (Optional[str]): SQL implementing the "down" or "reverse" migration.
    """

    app: str
    ts: int = field(default_factory=migration_timestamp)
    name: str = field(default_factory=str)
    depends: list[str] = field(default_factory=list)
    applied: Optional[datetime] = None
    doc: Optional[str] = None
    up: Optional[str] = None
    dn: Optional[str] = None

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

    def __repr__(self) -> str:
        return (
            self.__class__.__name__
            + "("
            + ", ".join(
                f"{key}={getattr(self, key)!r}" for key in ["key", "depends", "applied"]
            )
            + ")"
        )

    def __str__(self) -> str:
        """The string representation of the Migration is the instance as YAML."""
        return self.yaml()

    def __hash__(self) -> int:
        """The unique hash is based on the Migration.key."""
        return uuid.uuid3(SQLY_UUID_NAMESPACE, self.key).int

    def dict(
        self, exclude: Optional[list] = None, exclude_none: bool = False
    ) -> Dict[str, Any]:
        """
        The Migration serialized as a dict.

        Arguments:
            exclude (Optional[list]): A list of fields to exclude.
            exclude_none (bool): Whether to exclude fields with value None.
        """
        return {
            key: val
            for key, val in asdict(self).items()
            if key not in (exclude or []) and (exclude_none is False or val is not None)
        }

    @property
    def key(self):
        """
        The Migration.key uniquely identifies the migration.
        Format = `{app}:{ts}_{name}`
        """
        return f"{self.app}:{self.ts}_{self.name}"

    @property
    def filename(self):
        """The filename (without path) for the Migration"""
        return f"{self.ts}_{self.name}.yaml"

    @classmethod
    def load(cls, filepath: str) -> Migration:
        """
        Load the migration at the given file path.
        """
        with open(filepath) as f:
            data = yaml.safe_load(f.read())

        return cls(**data)

    @classmethod
    def key_load(cls, migration_key: str) -> Migration:
        """Load the Migration that has the given key."""
        return cls.load(cls.key_filepath(migration_key))

    @classmethod
    def key_filepath(cls, migration_key: str) -> Path:
        """The file path of the Migration that has the given key.

        Arguments:
            migration_key (str): The Migration key

        Returns:
            file path (Path): The files file of the Migration
        """
        app, basename = migration_key.split(":")
        return app_migrations_path(app) / f"{basename}.yaml"

    @classmethod
    def app_migrations(
        cls, app: str, include_depends: bool = True
    ) -> Dict[str, Migration]:
        """
        For a given module name, get the migrations in that module. If `include_depends`
        is `True` (the default), also include depends migrations from other apps.

        Arguments:
            app (str): The name of the app (module) for which to list migrations.
            include_depends (bool): Whether to include dependency Migrations in the
                listing.

        Returns:
            migrations (dict[str, Migration]): A dict of Migrations, by key.
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
    def all_migrations(cls, *apps: list[str]) -> Dict[str, Migration]:
        """
        Return all the migrations, including dependencies, for the given app(s).

        Arguments:
            apps (list[str]): The app or apps for which to list Migrations.

        Returns:
            migrations (dict[str, Migration]): A dict of Migrations, by key.
        """
        # always depend on sqly
        migrations = cls.app_migrations("sqly")
        for app in [app for app in apps if app not in ["sqly"]]:
            migrations |= cls.app_migrations(app, include_depends=True)

        return migrations

    @classmethod
    def create(
        cls, app: str, *other_apps: list[str], name: Optional[str] = None
    ) -> Migration:
        """
        Create a new Migration object for the given app (module) name. The new Migration
        is not saved to the filesystem: It is just a Migration instance in memory.

        Every new migration automatically depends on all the "leaf" nodes in the
        existing migration graph. Leaf nodes are those with out_degree == 0 (no edges
        pointing out). See:
        <https://networkx.org/documentation/stable/reference/classes/generated/networkx.DiGraph.out_degree.html>.
        For a worked example, see:
        <https://stackoverflow.com/questions/31946253/find-end-nodes-leaf-nodes-in-radial-tree-networkx-graph/31953001>.

        NOTE: The existing migration graph is calculated from the filesystem, not what
        is applied in any database. Migrations from other branches might currently be
        applied in the database; but for the purpose of creating a Migration graph, the
        filesystem is the source of truth.

        Arguments:
            app (str): The name of the app for which to create the new Migration.
            other_apps (list[str]): The other apps to include in the dependency graph.
            name (Optional[str]): The name (label) for the migration. Default = `""`.

        Returns:
            migration (Migration): The Migration that has just been created.
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
    def database_migrations(cls, connection: Any, dialect: Dialect) -> Dict[str, Migration]:
        """
        Query the database with the given `connection` and return a dict of the
        Migrations in the database, by key. If no Migrations have been applied in the
        database, the result is an empty dict.

        Arguments:
            connection (Any): A database connection.

        Returns:
            migrations (dict[str, Migration]): A dict of Migrations by key.
        """
        sql = SQL(dialect=dialect)
        try:
            results = sql.select(connection, "select * from sqly_migrations")
            records = list(results)

        except Exception as exc:
            print(str(exc))
            records = []

        return {m.key: m for m in set(cls(**record) for record in records)}

    @classmethod
    def migrate(
        cls,
        connection: Any,
        dialect: Dialect,
        migration: Migration,
        dryrun: bool = False,
    ):
        """
        Migrate the database to this migration, either up or down, using the given
        database connection.

        Algorithm:

        1. Collate the list of applied migrations in the database with the list of
           migrations available in this application.

        2. Calculate the graph path to reach this migration and whether this is an "up"
           or "down" migration.
            - if this migration has not been applied to the database, then the graph
              path is from the last applied predecessor "up" to this migration.
            - if this migration has been applied to the database, then the graph path is
              from the last applied successor "down" to this migration.

        3. Apply the sequence of migrations (either up or down).

        [_What about situations in which the path to the given Migration includes both
        "down" Migrations to back out of another branch and "up" Migrations preceding
        the given Migration on its branch? Our current solution is to ignore "other"
        branches and only migrate from the last applied predecessor._]

        Arguments:
            connection (Any): A database connection. dialect (Dialect): The SQL database
            migration (Migration): The Migration that we are migrating _to_.
            dryrun (bool): Whether this is a dry run.
        """
        db_migrations = cls.database_migrations(connection, dialect)
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

    def depends_migrations(self) -> Dict[str, Migration]:
        """
        All migrations that this migration depends on, recursively.

        Returns:
            migrations (dict[str, Migration]): A dict of Migrations by key.
        """
        dependencies = {}
        for depend in self.depends:
            migration = self.key_load(depend)
            dependencies |= {depend: migration} | migration.depends_migrations()
        return dependencies

    @classmethod
    def graph(cls, migrations: Mapping[str, Migration]) -> nx.classes.digraph.DiGraph:
        """
        Given a mapping of Migrations, create a dependency graph of Migrations. The
        resulting graph is a DAG (directed acyclic graph) that is a [transitive
        reduction](https://en.wikipedia.org/wiki/Transitive_reduction) of the Migrations
        graph. If the graph is not a DAG (e.g., it has cycles) then a networkx.HasACycle
        exception is raised.

        Arguments:
            migrations (Mapping[str, Migration]): A mapping of Migrations by key.

        Returns:
            graph (nx.classes.digraph.DiGraph): A networkx DiGraph of the Migrations.
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

    def ancestors(self, graph: nx.classes.digraph.DiGraph) -> AbstractSet[str]:
        """
        Given a Migration and a graph, return the set of all ancestors of this
        Migration. If this Migration is not in the given graph, a NetworkXError
        Exception is raised.

        Arguments:
            graph (nx.classes.digraph.DiGraph): A graph of Migrations including this
                one.

        Returns:
            migration keys (set): The set of migrations (keys) that are ancestors.
        """
        return nx.ancestors(graph, self.key)

    def descendants(self, graph: nx.classes.digraph.DiGraph) -> AbstractSet[str]:
        """
        Given a Migration and a graph, return the set of all descendants of this
        Migration. If this Migration is not in the given graph, a NetworkXError
        Exception is raised.

        Arguments:
            graph (nx.classes.digraph.DiGraph): A graph of Migrations including this
                one.

        Returns:
            migration keys (set): The set of migrations (keys) that are ancestors.
        """
        return nx.descendants(graph, self.key)

    def yaml(self, exclude: Optional[list] = None, exclude_none: bool = False) -> str:
        """
        Serialize this Migration as a YAML string.

        Arguments:
            exclude (Optional[list]): A list of fields to exclude.
            exclude_none (bool): Whether to exclude fields with value None.
        """
        return yaml.dump(
            self.dict(exclude=exclude, exclude_none=exclude_none),
            default_flow_style=False,
            sort_keys=False,
        )

    def save(self, exclude: Optional[list] = None, exclude_none: bool = False):
        """
        Save this Migration to the filesystem.

        Arguments:
            exclude (Optional[list]): A list of fields to exclude.
            exclude_none (bool): Whether to exclude fields with value None.

        Returns:
            tuple (filepath, size): The filepath where the Migration was saved, and its
                size in bytes.
        """
        filepath = app_migrations_path(self.app) / self.filename
        os.makedirs(filepath.parent, exist_ok=True)
        with open(filepath, "wb") as f:
            size = f.write(
                self.yaml(exclude=exclude, exclude_none=exclude_none).encode()
            )

        return filepath, size

    def apply(
        self,
        connection: Any,
        dialect: Dialect,
        direction: str = "up",
        dryrun: bool = False,
    ):
        """
        Apply the migration (direction = 'up' or 'dn') to connection database. The
        entire migration script is wrapped in a transaction. (This method is called
        internally by `Migration.migrate()`).

        Arguments:
            connection (Any): A database connection.
            dialect (Dialect): The SQL dialect of the database connection.
            direction (str): Which migration to apply: "up" or "dn".
            dryrun (bool): Whether this is a dry run.
        """
        print(self.key, direction, end=" ... ")

        if dryrun:
            print("DRY RUN")
            return

        migration_query = getattr(self, direction)
        if migration_query:
            connection.execute(migration_query)

        if direction == "up":
            sqly_migrations_query = self.insert_query(dialect)
        else:
            sqly_migrations_query = self.delete_query(dialect)

        connection.execute(*sqly_migrations_query)
        connection.commit()
        print("OK")

    def insert_query(self, dialect: Dialect) -> Any:
        """
        Render a SQL query to insert this Migration into the sqly_migrations table.

        Arguments:
            dialect (Dialect): The SQL database dialect to render the query for.

        Returns:
            tuple (str, params...): The SQL query and params formatted for the database
                dialect.
        """
        data = {k: v for k, v in self.dict(exclude_none=True).items()}
        data["depends"] = json.dumps(data.get("depends") or [])
        sql = queries.INSERT("sqly_migrations", data)
        return SQL(dialect=dialect).render(sql, data)

    def delete_query(self, dialect):
        """
        Render a SQL query to delete this Migration from the sqly_migrations table.

        Arguments:
            dialect (Dialect): The SQL database dialect to render the query for.

        Returns:
            tuple (str, params...): The SQL query and params formatted for the database
                dialect.
        """
        sql = queries.DELETE(
            "sqly_migrations", [Q.filter(key) for key in ["app", "ts", "name"]]
        )
        return SQL(dialect=dialect).render(sql, self.dict())
