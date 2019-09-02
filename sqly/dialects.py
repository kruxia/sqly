import json
import re
from enum import Enum


class Dialects(Enum):
    # the value of each Dialect represents the syntax that it uses for query parameters
    EMBEDDED = ':field'
    MYSQL = '%(field)s'
    SQLITE = '?'
    POSTGRES = '$i'

    ASYNCPG = POSTGRES
    PSYCOPG2 = MYSQL
    SQLALCHEMY = EMBEDDED

    def render(self, query_string, data):
        """render a query_string and its parameter values for this dialect."""
        pattern = r"(?<!\\):(\w+)\b"
        fields = []

        def replace_parameter(match):
            field = match.group(1)
            if self in [self.SQLITE, self.POSTGRES] or field not in fields:
                fields.append(field)

            if self is self.EMBEDDED:
                return f":{field}"
            elif self is self.MYSQL:
                return f"%({field})s"
            elif self is self.SQLITE:
                return "?"
            elif self is self.POSTGRES:
                return f"${len(fields)}"
            else:
                raise ValueError('Dialect %r not in Dialects' % self.dialect)

        if self is self.MYSQL:
            # any % must be intended as literal and must be doubled
            query_string = query_string.replace('%', '%%')

        rendered_query_string = re.sub(pattern, replace_parameter, query_string)

        if self is not self.EMBEDDED:
            # replace \:word with :word because the colon-escape is no longer needed.
            rendered_query_string = re.sub(r"\\:(\w+)\b", r":\1", rendered_query_string)

        if self in [self.EMBEDDED, self.MYSQL]:
            parameter_values = {
                key: json.dumps(val) if isinstance(val, dict) else val
                for key, val in {field: data[field] for field in fields}.items()
            }
        elif self in [self.SQLITE, self.POSTGRES]:
            parameter_values = [
                json.dumps(val) if isinstance(val, dict) else val
                for val in [data[field] for field in fields]
            ]

        return rendered_query_string, parameter_values
