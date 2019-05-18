from dataclasses import dataclass, field
from .lib import flatten
from .params import Params


@dataclass
class SQL:
    query: list = field(default_factory=list)
    params: dict = field(default_factory=Params)
    dialect: str = field(default=':var')

    def render(self, params=None, keys=None, vals=None, dialect=None):
        if params is None:
            params = self.params
        if keys is None:
            keys = params.keys()
        if vals is None:
            vals = params.keys()
        if dialect is None:
            dialect = self.dialect

        print(params, keys, vals, dialect)

        sql = " ".join(flatten(self.query))
        rendered = sql.format(
            fields=params.fields(keys=vals, dialect=dialect),
            params=params.params(keys=vals, dialect=dialect),
            updates=params.updates(keys=vals, dialect=dialect),
            wheres=params.wheres(keys=keys, dialect=dialect),
        )

        return rendered
