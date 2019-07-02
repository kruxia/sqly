from dataclasses import dataclass, field
from .dialects import Dialects
from .lib import walk_list


@dataclass
class SQL:
    query: list = field(default_factory=list)
    fields: list = field(default_factory=list)
    relation: str = field(default='')
    keys: list = field(default_factory=list)
    updates: list = field(default_factory=list)
    dialect: str = field(default=Dialects.EMBEDDED)

    def __post_init__(self):
        # allow the query to be a single string
        if not (isinstance(self.query, list)):
            self.query = [self.query]

    def __str__(self):
        return self.render()

    def render(self):
        query_string = ' '.join([str(q) for q in walk_list(self.query)])
        rendered = query_string.format(
            fields=', '.join(self.fields),
            relation=self.relation,
            keys=', '.join(self.keys),
            vars=self.vars_string(),
            filters=self.filters_string(),
            updates=self.updates_string(),
        )
        return rendered

    def vars_string(self):
        if self.dialect in Dialects.POSTGRES.value:
            items = ["$%d" % index for index in range(1, len(self.fields) + 1)]
        elif self.dialect in Dialects.MYSQL.value:
            items = ["%%(%s)s" % field for field in self.fields]
        elif self.dialect in Dialects.EMBEDDED.value:
            items = [":%s" % field for field in self.fields]
        elif self.dialect in Dialects.SQLITE.value:
            items = ["?" for field in self.fields]
        else:
            raise ValueError('Dialect %r not in Dialects' % self.dialect)

        return ", ".join(items)

    def updates_string(self):
        return ", ".join(self.assignments(fields=self.updates))

    def filters_string(self):
        return " AND ".join(self.assignments(fields=self.keys))

    def assignments(self, fields=None):
        if fields is None:
            fields = self.fields

        if self.dialect in Dialects.POSTGRES.value:
            items = ["%s=$%d" % (field, list(self.fields).index(field) + 1) for field in fields]
        elif self.dialect in Dialects.MYSQL.value:
            items = ["%s=%%(%s)s" % (field, field) for field in fields]
        elif self.dialect in Dialects.EMBEDDED.value:
            items = ["%s=:%s" % (field, field) for field in fields]
        elif self.dialect in Dialects.SQLITE.value:
            items = ["%s=?" % field for field in fields]
        else:
            raise ValueError('Dialect %r not in Dialects' % self.dialect)

        return items
