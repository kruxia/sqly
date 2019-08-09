from dataclasses import dataclass, field
from .dialects import Dialects
from .lib import walk_list


@dataclass
class SQL:
    query: list = field(default_factory=list)
    dialect: str = Dialects.EMBEDDED
    fields: list = field(default_factory=list)
    keys: list = field(default_factory=list)

    def __post_init__(self):
        # allow the query to be a single string
        if isinstance(self.query, str):
            self.query = [self.query]

    def __str__(self):
        return self.render()

    def render(self):
        query_string = ' '.join([str(q) for q in walk_list(self.query)])
        return query_string.format(
            keys=', '.join(self.keys),
            filters=' AND '.join(self.assignments(self.keys)),
            fields=', '.join(self.fields),
            vars=', '.join(self.vars(self.fields)),
            updates=', '.join(self.assignments(self.fields)),
        )

    def vars(self, fields=None):
        if not fields:
            fields = self.fields

        if self.dialect not in Dialects:
            raise ValueError('Dialect %r not in Dialects' % self.dialect)
        if self.dialect == Dialects.POSTGRES:
            return ["$%d" % index for index in range(1, len(fields) + 1)]
        if self.dialect == Dialects.MYSQL:
            return ["%%(%s)s" % field for field in fields]
        if self.dialect == Dialects.EMBEDDED:
            return [":%s" % field for field in fields]
        if self.dialect == Dialects.SQLITE:
            return ["?" for field in fields]

    def assignments(self, fields=None):
        if not fields:
            fields = self.fields

        if self.dialect not in Dialects:
            raise ValueError('Dialect %r not in Dialects' % self.dialect)
        if self.dialect == Dialects.EMBEDDED:
            return ["%s=:%s" % (field, field) for field in fields]
        if self.dialect == Dialects.POSTGRES:
            return [
                "%s=$%d" % (field, list(fields).index(field) + 1) for field in fields
            ]
        if self.dialect == Dialects.MYSQL:
            return ["%s=%%(%s)s" % (field, field) for field in fields]
        if self.dialect == Dialects.SQLITE:
            return ["%s=?" % field for field in fields]
