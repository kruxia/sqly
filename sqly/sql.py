from dataclasses import dataclass, field

from .dialects import Dialects
from .lib import walk_list


@dataclass
class SQL:
    query: list = field(default_factory=list)
    dialect: str = Dialects.EMBEDDED

    def __post_init__(self):
        # allow the query to be a single string
        if isinstance(self.query, str):
            self.query = [self.query]

    def __str__(self):
        return ' '.join([str(q) for q in walk_list(self.query)])

    def render(
        self,
        data,
        fields=None,
        keys=None,
        filters=None,
        assigns=None,
        params=None,
        dialect=None,
        **kwargs
    ):
        """
        Render the query and its values for a given data input.
        """
        # Format the query string
        keys = keys or data.keys()
        fields = fields or data.keys()
        query_string = str(self)
        formatted_query = query_string.format(
            keys=', '.join(keys),
            fields=', '.join(fields),
            filters=' AND '.join(filters or self.assigns_list(keys)),
            assigns=', '.join(assigns or self.assigns_list(fields)),
            params=', '.join(params or self.params_list(fields)),
            **kwargs
        )
        # Render the query and parameter value with the correct syntax for the dialect
        dialect = dialect or self.dialect
        rendered_query, parameter_values = dialect.render(formatted_query, data)
        return rendered_query, parameter_values

    def params_list(self, fields=None):
        if not fields:
            fields = self.fields
        return [":%s" % field for field in fields]

    def assigns_list(self, fields=None):
        if not fields:
            fields = self.fields
        return ["%s=:%s" % (field, field) for field in fields]
