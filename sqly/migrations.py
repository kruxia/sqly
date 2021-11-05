from datetime import datetime, timezone
from typing import List

import networkx as nx
import yaml
from pydantic import BaseModel, Field


def make_migration_id():
    return datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S%f')[:-3]  # milliseconds


class Migration(BaseModel):
    id: str = Field(default_factory=make_migration_id)
    name: str = Field(default='')
    depends: List[str] = Field(default_factory=list)
    applied: datetime = Field(default=None)
    doc: str = Field(default=None)
    up: str = Field(default=None)
    dn: str = Field(default=None)

    @property
    def filename(self):
        return f'{self.id}_{self.name}.yaml'

    def serialize(self):
        return yaml.dump(self.dict(), default_flow_style=False, sort_keys=False)

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
