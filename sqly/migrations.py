from datetime import datetime, timezone
from glob import glob
from importlib import import_module
from pathlib import Path
from typing import List, Tuple

import networkx as nx
import yaml
from pydantic import BaseModel, Field


def app_migrations_path(app):
    """
    For a given app name, get the path to the migrations directory
    """
    mod = import_module(app)
    mod_filepath = Path(mod.__file__).parent
    return mod_filepath / 'migrations'


def app_migrations(app):
    """
    For a given module name, get the existing migrations in that module.
    """
    migration_filenames = glob(str(app_migrations_path(app) / '*.yaml'))
    migrations = [Migration.load(filename) for filename in migration_filenames]
    return migrations


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
    up: List[str] = Field(default_factory=list)
    dn: List[str] = Field(default_factory=list)

    @property
    def key(self):
        """
        The Migration key is used to uniquely identify the migration in the dependency
        graph. The key has the structure "{app}:{id}"

        - app = the name of the app that the Migration is in
        - id = the integer id for the Migration in that app
        """
        return f"{self.app}:{self.id}"

    @property
    def filename(self):
        return f'{self.id}_{self.name or ""}.yaml'

    @classmethod
    def create(cls, app, name=None):
        """
        For information on the calculation of topological generations in a graph, see:
        <https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.topological_generations.html>
        """
        migrations = app_migrations('sqly') + app_migrations(app)
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
        Given an iterable of Migrations, create a dependency DAG of Migration ids, which
        can be used to determine
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

    def ancestors(self, graph):
        return nx.ancestors(graph, self.id)

    def descendants(self, graph):
        return nx.descendants(graph, self.id)

    def yaml(self, **kwargs):
        return yaml.dump(self.dict(**kwargs), default_flow_style=False, sort_keys=False)

    def save(self):
        filepath = app_migrations_path(self.app) / self.filename
        with open(filepath, 'wb') as f:
            size = f.write(self.yaml().encode())
        return filepath, size
