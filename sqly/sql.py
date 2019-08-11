from dataclasses import dataclass, field
from .dialects import Dialects
from .lib import walk_list


@dataclass
class SQL:
    query: list = field(default_factory=list)
    dialect: str = Dialects.EMBEDDED
    keys: list = field(default_factory=list)
    fields: list = field(default_factory=list)
    filters: list = field(default_factory=list)
    assigns: list = field(default_factory=list)
    params: list = field(default_factory=list)

    def __post_init__(self):
        # allow the query to be a single string
        if isinstance(self.query, str):
            self.query = [self.query]

    def __str__(self):
        return self.render()

    def render(self, data):
        """render the query and its values for a given data input."""
        query_string = ' '.join([str(q) for q in walk_list(self.query)])
        fields = self.fields or data.keys()
        rendered_query, parameter_values = self.dialect.render(
            query_string.format(
                keys=', '.join(self.keys),
                fields=', '.join(fields),
                filters=' AND '.join(self.filters or self.assigns_list(self.keys)),
                assigns=', '.join(self.assigns or self.assigns_list(fields)),
                params=', '.join(self.params or self.params_list(fields)),
            ),
            data,
        )
        return rendered_query, parameter_values

    def params_list(self, fields=None):
        if not fields:
            fields = self.fields
        return [":%s" % field for field in fields]

    def assigns_list(self, fields=None):
        if not fields:
            fields = self.fields
        return ["%s=:%s" % (field, field) for field in fields]
