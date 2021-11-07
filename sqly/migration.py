from datetime import datetime, timezone
from glob import glob
from importlib import import_module
from pathlib import Path
from typing import List, Tuple

import networkx as nx
import yaml
from pydantic import BaseModel, Field, validator


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
    name: str = Field(default=None)
    depends: List[str] = Field(default_factory=list)
    applied: datetime = Field(default=None)
    doc: str = Field(default=None)
    up: str = Field(default=None)
    dn: str = Field(default=None)

    @validator('depends', pre=True, always=True)
    def depends_default_empty_list(cls, value):
        return value or []

    def __repr__(self):
        return (
            'Migration('
            + ', '.join(
                f'{key}={getattr(self, key)!r}'
                for key in ['id', 'app', 'name', 'depends', 'applied']
            )
            + ')'
        )

    def __str__(self):
        return self.yaml()

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
    def app_migrations(cls, app):
        """
        For a given module name, get the existing migrations in that module.
        """
        migration_filenames = glob(str(app_migrations_path(app) / '*.yaml'))
        migrations = [cls.load(filename) for filename in migration_filenames]
        return migrations

    @classmethod
    def create(cls, app, name=None):
        """
        Every new migration depends on the previous generation of migration. (For
        information on the calculation of topological generations in a graph, see:
        <https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.topological_generations.html>)
        """
        migrations = cls.app_migrations('sqly') + cls.app_migrations(app)
        graph = cls.graph(migrations)
        gens = list(nx.topological_generations(graph))
        migration = cls(
            app=app,
            name=name or '',
            depends=gens[-1] if gens else [],
        )
        return migration

    @classmethod
    def load(cls, filename):
        with open(filename) as f:
            data = yaml.safe_load(f.read())
        return Migration.parse_obj(data)

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
            m.key: m for m in cls.app_migrations(migration.app)
        }
        graph = cls.graph(migrations.values())

        if migration.key not in db_migrations:
            subgraph = graph.subgraph(
                list(graph.predecessors(migration.key)) + [migration.key]
            )
            for key in list(nx.lexicographical_topological_sort(subgraph)):
                if key not in db_migrations:
                    migrations[key].apply(database, direction='up', connection=connection)
        else:
            subgraph = graph.subgraph(list(graph.successors(migration.key)))
            for key in reversed(list(nx.lexicographical_topological_sort(subgraph))):
                if key in db_migrations:
                    migrations[key].apply(database, direction='dn', connection=connection)

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
        print(self.key, self.name, direction, end=' ... ')
        connection = connection or database.connect()
        connection.execute('begin;')
        connection.executescript(getattr(self, direction) or '')
        if direction == 'up':
            connection.execute(*self.insert_query(database))
        else:
            connection.execute(*self.delete_query(database))
        connection.execute('commit;')
        print('OK')

    def insert_query(self, database):
        """
        Insert this migration into the sqly_migration table.
        """
        data = {k: v for k, v in self.dict().items() if v}
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
