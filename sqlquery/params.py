from dataclasses import dataclass, field
from .dialects import Dialects


@dataclass
class Params:
    data: dict = field(default_factory=dict)
    dialect: str = field(default=':var')

    def fields(self, keys=None, dialect=None):
        if keys is None:
            keys = self.data.keys()

        return ', '.join(key for key in keys)

    def params(self, keys=None, dialect=None):
        if keys is None:
            keys = self.data.keys()
        if dialect is None:
            dialect = self.dialect

        if dialect in Dialects.POSTGRES.value:
            items = ["$%d" % index + 1 for index in len(keys)]
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

    def wheres(self, keys=None, dialect=None):
        return " AND ".join(self.assignments(keys=keys, dialect=dialect))

    def assignments(self, keys=None, dialect=None):
        if keys is None:
            keys = self.data.keys()
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
            keys = self.data.keys()

        return (key for key in self.data.keys() if key in keys)

    def values(self, keys=None):
        if keys is None:
            keys = self.data.keys()

        return (value for key, value in self.data.items() if key in keys)

    def items(self, keys=None):
        if keys is None:
            keys = self.data.keys()
        return ((key, value) for key, value in self.data.items() if key in keys)

    def dict(self, keys=None):
        if keys is None:
            keys = self.data.keys()

        return {key: value for key, value in self.data.items() if key in keys}
