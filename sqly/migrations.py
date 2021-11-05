from datetime import datetime, timezone
from glob import glob
from importlib import import_module
from pathlib import Path
from typing import List

import networkx as nx
import yaml
from pydantic import BaseModel, Field


def get_migrations_path(mod_name):
    """
    For a given module name, get the path to the migrations directory
    """
    mod = import_module(mod_name)
    mod_filepath = Path(mod.__file__).parent
    return mod_filepath / 'migrations'    


def get_migrations(mod_name):
    """
    For a given module name, get the existing migrations in that module.
    """
    migration_filenames = glob(str(get_migrations_path(mod_name) / '*.yaml'))
    migrations = [Migration.load(filename) for filename in migration_filenames]
    return migrations


def make_migration_id():
    return datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S%f')[:-3]  # milliseconds


def make_migration(mod_name, name=None):
    migrations = get_migrations(mod_name)
    graph = Migration.make_graph(migrations)
    lts = list(nx.lexicographical_topological_sort(graph))  
    m = Migration(
        app=mod_name, 
        name=name or '',
        depends=lts[-1:],
    )
    return m


def save_migration(m):
    filepath = get_migrations_path(m.app) / m.filename
    with open(filepath, 'wb') as f:
        size = f.write(m.serialize().encode())
    return filepath, size


class Migration(BaseModel):
    id: str = Field(default_factory=make_migration_id)
    app: str
    name: str = Field(default=None)
    depends: List[str] = Field(default_factory=list)
    applied: datetime = Field(default=None)
    doc: str = Field(default=None)
    up: str = Field(default=None)
    dn: str = Field(default=None)

    @property
    def filename(self):
        return f'{self.id}_{self.name or ""}.yaml'

    def serialize(self):
        return yaml.dump(self.dict(), default_flow_style=False, sort_keys=False)

    @classmethod
    def load(cls, filename):
        with open(filename) as f:
            data = yaml.safe_load(f.read())
        return Migration.parse_obj(data)

    @classmethod
    def make_graph(cls, migrations):
        """
        Given an iterable of Migrations, create a dependency DAG of Migration ids, which
        can be used to determine
        """
        graph = nx.DiGraph()
        dag = {m.id: m.depends for m in migrations}
        for migration_id, migration_depends in dag.items():
            graph.add_node(migration_id)
            for depend_id in migration_depends:
                graph.add_edge(depend_id, migration_id)
        if not nx.is_directed_acyclic_graph(graph):
            raise nx.HasACycle(dag)
        return nx.transitive_reduction(graph)

    def ancestors(self, graph):
        return nx.ancestors(graph, self.id)

    def descendants(self, graph):
        return nx.descendants(graph, self.id)
