from dataclasses import dataclass, field
from .dialects import Dialects
from .lib import flatten


@dataclass
class SQL:
    query: list = field(default_factory=list)
    params: dict = field(default_factory=dict)
    dialect: str = field(default=':var')
    pk: list = field(default_factory=list)
    relation: str = field(default='')

    def __post_init__(self):
        # allow the query to be a single string
        if not (isinstance(self.query, list)):
            self.query = [self.query]

    def __str__(self):
        return self.render()

    def render(self,
               params=None,
               keys=None,
               varnames=None,
               values=None,
               relation=None,
               dialect=None):
        if params is None:
            params = self.params
        if keys is None:
            keys = self.pk or list(params.keys())
        if varnames is None:
            varnames = list(params.keys())
        if values is None:
            values = list(val for key, val in params.items())
        if relation is None:
            relation = self.relation
        if dialect is None:
            dialect = self.dialect

        sql = ' '.join([str(q) for q in flatten(self.query)])
        rendered = sql.format(
            relation=relation,
            keys=', '.join(keys),
            filters=self.filters(keys=keys, dialect=dialect),
            varnames=self.varnames(keys=varnames, dialect=dialect),
            fields=self.fields(keys=varnames, dialect=dialect),
            updates=self.updates(keys=varnames, dialect=dialect),
        )

        return rendered

    def fields(self, keys=None, dialect=None):
        if keys is None:
            keys = list(self.params.keys())
        return ', '.join(key for key in keys)

    def varnames(self, keys=None, dialect=None):
        if keys is None:
            keys = list(self.params.keys())
        if dialect is None:
            dialect = self.dialect

        if dialect in Dialects.POSTGRES.value:
            items = ["$%d" % index for index in range(1, len(keys) + 1)]
        elif dialect in Dialects.MYSQL.value:
            items = ["%%(%s)s" % key for key in keys]
        elif dialect in Dialects.EMBEDDED.value:
            items = [":%s" % key for key in keys]
        elif dialect in Dialects.SQLITE.value:
            items = ["?" for key in keys]
        else:
            raise ValueError('Dialect %r not in Dialects' % dialect)

        return ", ".join(items)

    def updates(self, keys=None, dialect=None):
        return ", ".join(self.assignments(keys=keys, dialect=dialect))

    def filters(self, keys=None, dialect=None):
        return " AND ".join(self.assignments(keys=keys, dialect=dialect))

    def assignments(self, keys=None, dialect=None):
        if keys is None:
            keys = list(self.params.keys())
        if dialect is None:
            dialect = self.dialect

        if dialect in Dialects.POSTGRES.value:
            items = ["%s=$%d" % (key, index + 1) for index, key in enumerate(keys)]
        elif dialect in Dialects.MYSQL.value:
            items = ["%s=%%(%s)s" % (key, key) for key in keys]
        elif dialect in Dialects.EMBEDDED.value:
            items = ["%s=:%s" % (key, key) for key in keys]
        elif dialect in Dialects.SQLITE.value:
            items = ["%s=?" % key for key in keys]
        else:
            raise ValueError('Dialect %r not in Dialects' % dialect)

        return items

    # standard mapping methods, optionally filtered

    def keys(self, keys=None):
        if keys is None:
            keys = list(self.params.keys())
        return [key for key in self.params.keys() if key in keys]

    def values(self, keys=None):
        if keys is None:
            keys = list(self.params.keys())
        return [value for key, value in self.params.items() if key in keys]

    def items(self, keys=None):
        if keys is None:
            keys = list(self.params.keys())
        return [(key, value) for key, value in self.params.items() if key in keys]

    def dict(self, keys=None):
        if keys is None:
            keys = list(self.params.keys())
        return {key: value for key, value in self.params.items() if key in keys}
