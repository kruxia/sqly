from enum import Enum
import re


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
        pattern = r":(\w+)\b"
        fields = []

        def replace(match):
            field = match.group(1)
            if field not in fields:
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

        rendered_query_string = re.sub(pattern, replace, query_string)

        if self in [self.EMBEDDED, self.MYSQL]:
            parameter_values = {field: data[field] for field in fields}
        elif self in [self.SQLITE, self.POSTGRES]:
            parameter_values = [data[field] for field in fields]

        return rendered_query_string, parameter_values
