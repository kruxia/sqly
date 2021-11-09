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

from .settings import SQLY_UUID_NAMESPACE


def app_migrations_path(app):
    """
    For a given app name, get the path to the migrations directory.
    """
    mod = import_module(app)
    mod_filepath = Path(mod.__file__).parent
    return mod_filepath / 'migrations'


def make_migration_id():
    """
    An integer with the timestamp to millisecond resolution (17 digits => bigint)
    """
    return int(datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S%f')[:-3])


class Migration(BaseModel):
    id: int = Field(default_factory=make_migration_id)
    app: str
    name: str = Field(default='')
    depends: List[str] = Field(default_factory=list)
    applied: datetime = Field(default=None)
    doc: str = Field(default=None)
    up: str = Field(default=None)
    dn: str = Field(default=None)

    @validator('depends', pre=True, always=True)
    def depends_default_empty_list(cls, value):
        if isinstance(value, str):
            return json.loads(value)
        else:
            return value or []

    def __repr__(self):
        return (
            'Migration('
            + ', '.join(
                f'{key}={getattr(self, key)!r}' for key in ['key', 'depends', 'applied']
            )
            + ')'
        )

    def __str__(self):
        return self.yaml()

    def __hash__(self):
        return uuid.uuid3(SQLY_UUID_NAMESPACE, self.key).int

    @property
    def key(self):
        """
        The Migration key is used to uniquely identify the migration in the dependency
        graph. The key has the structure "{app}:{id}_{name}"

        - app = the name of the app that the Migration is in
        - id = the integer id for the Migration in that app
        """
        return f"{self.app}:{self.id}_{self.name or ''}"

    @property
    def filename(self):
        return f'{self.id}_{self.name or ""}.yaml'

    @classmethod
    def load(cls, filepath):
        with open(filepath) as f:
            data = yaml.safe_load(f.read())

        return Migration.parse_obj(data)

    @classmethod
    def key_load(cls, migration_key):
        return cls.load(cls.key_filepath(migration_key))

    @classmethod
    def key_filepath(cls, migration_key):
        app, basename = migration_key.split(':')
        return app_migrations_path(app) / f"{basename}.yaml"

    @classmethod
    def app_migrations(cls, app, include_depends=True):
        """
        For a given module name, get the existing migrations in that module. If
        include_depends is True (default), also include depends migrations from any app.
        """
        migration_filenames = glob(str(app_migrations_path(app) / '*.yaml'))
        migrations = set(cls.load(filename) for filename in migration_filenames)
        if include_depends is True:
            dms = set()
            for migration in migrations:
                dms |= migration.depends_migrations()

            migrations |= dms

        return migrations

    @classmethod
    def all_migrations(cls, *apps):
        migrations = set()
        for app in apps:
            migrations |= cls.app_migrations(app)

        return migrations

    @classmethod
    def create(cls, app, *other_apps, name=None):
        """
        Every new migration depends on all the "leaf" nodes in the existing migration
        graph. Leaf nodes are those with out_degree == 0 (no edges pointing out). See:
        <https://networkx.org/documentation/stable/reference/classes/generated/networkx.DiGraph.out_degree.html>.
        For a worked example, see:
        <https://stackoverflow.com/questions/31946253/find-end-nodes-leaf-nodes-in-radial-tree-networkx-graph/31953001>.
        """
        migrations = cls.all_migrations(app, *other_apps)
        graph = cls.graph(migrations)
        depends = [node for node in graph.nodes() if graph.out_degree(node) == 0]
        name = re.sub(r'\W+', '_', name or '')
        migration = cls(
            app=app, name=name or '', depends=depends, doc=None, up=None, dn=None
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
    def migrate(cls, database, migration, connection=None):
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
        connection = connection or database.connect()
        try:
            cursor = connection.execute(
                "select * from sqly_migration where applied is not null"
            )
            fields = [d[0] for d in cursor.description]
            db_migrations = {
                m.key: m
                for m in [
                    cls(**record)
                    for record in [dict(zip(fields, row)) for row in cursor]
                ]
            }
        except Exception as exc:
            print(exc)
            db_migrations = {}

        migrations = db_migrations | {
            m.key: m for m in cls.all_migrations(migration.app)
        }
        graph = cls.graph(migrations.values())

        if migration.key not in db_migrations:
            subgraph = graph.subgraph(
                list(graph.predecessors(migration.key)) + [migration.key]
            )
            for key in list(nx.lexicographical_topological_sort(subgraph)):
                if key not in db_migrations:
                    migrations[key].apply(
                        database, direction='up', connection=connection
                    )
        else:
            subgraph = graph.subgraph(list(graph.successors(migration.key)))
            for key in reversed(list(nx.lexicographical_topological_sort(subgraph))):
                if key in db_migrations:
                    migrations[key].apply(
                        database, direction='dn', connection=connection
                    )

    def depends_migrations(self):
        """
        All migrations that this migration depends on, recursively.
        """
        dms = set()
        for depend in self.depends:
            depend_migration = self.load(self.key_filepath(depend))
            dms |= {depend_migration} | depend_migration.depends_migrations()
        return dms

    def ancestors(self, graph):
        return nx.ancestors(graph, self.id)

    def descendants(self, graph):
        return nx.descendants(graph, self.id)

    def yaml(self, **kwargs):
        return yaml.dump(self.dict(**kwargs), default_flow_style=False, sort_keys=False)

    def save(self):
        filepath = app_migrations_path(self.app) / self.filename
        os.makedirs(filepath.parent, exist_ok=True)
        with open(filepath, 'wb') as f:
            size = f.write(self.yaml().encode())

        return filepath, size

    def apply(self, database, direction='up', connection=None):
        """
        Apply the migration (direction = 'up' or 'dn') to connection database. The
        entire migration script is wrapped in a transaction.
        """
        print(self.key, direction, end=' ... ')
        connection = connection or database.connect()
        connection.execute('begin;')
        connection.executescript(getattr(self, direction) or '')
        if direction == 'up':
            query = self.insert_query(database)
        else:
            query = self.delete_query(database)

        connection.execute(*query)
        connection.execute('commit;')
        print('OK')

    def insert_query(self, database):
        """
        Insert this migration into the sqly_migration table.
        """
        data = {k: v for k, v in self.dict(exclude_none=True).items()}
        if not isinstance(data.get('depends'), str):
            data['depends'] = json.dumps(data.get('depends') or [])
        keys = [k for k in data.keys()]
        params = [f':{k}' for k in keys]
        sql = f"""
            INSERT INTO sqly_migration ({','.join(keys)})
            VALUES ({','.join(params)});
            """
        return database.dialect.render(sql, data)

    def delete_query(self, database):
        sql = "DELETE FROM sqly_migration where id=:id"
        return database.dialect.render(sql, self.dict())
