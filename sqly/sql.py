from dataclasses import dataclass, field

from sqly.dialects import DEFAULT_DIALECT
from sqly.lib import walk_list


@dataclass
class SQL:
    query: list = field(default_factory=list)
    dialect: str = DEFAULT_DIALECT

    def __post_init__(self):
        # allow the query to be a single string
        if isinstance(self.query, str):
            self.query = [self.query]

    def __str__(self):
        return ' '.join([str(q) for q in walk_list(self.query) if q]).strip()

    def render(
        self,
        data,
        fields=None,
        keys=None,
        filters=None,
        assigns=None,
        params=None,
        dialect=None,
        **kwargs,
    ):
        """
        Render the query and its values for a given data input.
        """
        # Format the query string
        if dialect is None:
            dialect = self.dialect or DEFAULT_DIALECT
        if keys is None:
            keys = data.keys()
        if fields is None:
            fields = data.keys()
        if filters is None:
            filters = self.assigns_list(keys)
        if assigns is None:
            assigns = self.assigns_list(fields)
        if params is None:
            params = self.params_list(fields)
        query_string = str(self)
        formatted_query = query_string.format(
            keys=', '.join(keys),
            fields=', '.join(fields),
            filters=' AND '.join(filters),
            where=f"where {' AND '.join(filters)}" if filters else '',
            assigns=', '.join(assigns),
            params=', '.join(params),
            assigns_excluded=', '.join(
                f'{key}=EXCLUDED.{key}' for key in fields if key not in keys
            ),
            **kwargs,
        )
        # Render the query and parameter value with the correct syntax for the dialect
        rendered_query, parameter_values = dialect.render(formatted_query, data)
        return rendered_query, parameter_values

    def params_list(self, fields):
        return [":%s" % field for field in fields]

    def assigns_list(self, fields):
        return ["%s=:%s" % (field, field) for field in fields]
