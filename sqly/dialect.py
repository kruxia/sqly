import json
import re
from enum import Enum
from importlib import import_module


class OutputFormats(Enum):
    COLON = ':field'
    PERCENT = '%(field)s'
    QMARK = '?'
    NUMBERED = '$i'


class Dialect(Enum):
    # the value of each Dialect represents the syntax that it uses for query parameters
    EMBEDDED = 'embedded'
    MYSQL = 'mysql'
    SQLITE = 'sqlite'
    POSTGRES = 'postgres'
    ASYNCPG = 'asyncpg'
    PSYCOPG2 = 'psycopg2'
    SQLALCHEMY = 'sqlalchemy'
    PYODBC = 'pyodbc'

    @property
    def supports_returning(self):
        return self in [self.POSTGRES, self.PSYCOPG2, self.ASYNCPG]

    @property
    def output_format(self):
        return {
            self.EMBEDDED: OutputFormats.COLON,
            self.SQLALCHEMY: OutputFormats.COLON,
            self.MYSQL: OutputFormats.PERCENT,
            self.PSYCOPG2: OutputFormats.PERCENT,
            self.SQLITE: OutputFormats.QMARK,
            self.POSTGRES: OutputFormats.NUMBERED,
            self.ASYNCPG: OutputFormats.NUMBERED,
            self.PYODBC: OutputFormats.QMARK,
        }[self]

    @property
    def adaptor(self):
        return {
            self.EMBEDDED: 'sqlalchemy',
            self.SQLALCHEMY: 'sqlalchemy',
            self.SQLITE: 'sqlite3',
            self.MYSQL: 'mysql',
            self.ASYNCPG: 'asyncpg',
            self.PSYCOPG2: 'psycopg2',
            self.POSTGRES: 'pyodbc',
        }[self]

    def load_adaptor(self):
        return import_module(self.adaptor)

    def render(self, query_string, data):
        """Render a query_string and its parameter values for this dialect.

        Returns 2-tuple: (query: str, data: dict)
        """
        pattern = r"(?<!\\):(\w+)\b"
        fields = []  # ordered list of fields for positional outputs (closure)

        def replace_parameter(match):
            field = match.group(1)

            # Build the ordered fields list for positional outputs (fields in closure)
            if (
                self.output_format in [OutputFormats.QMARK, OutputFormats.NUMBERED]
                or field not in fields
            ):
                fields.append(field)

            # Return the field formatted for the output type
            if self.output_format == OutputFormats.COLON:
                return f":{field}"
            elif self.output_format == OutputFormats.PERCENT:
                return f"%({field})s"
            elif self.output_format == OutputFormats.QMARK:
                return "?"
            elif self.output_format == OutputFormats.NUMBERED:
                return f"${len(fields)}"
            else:
                raise ValueError(
                    'Dialect %r output_format %r not supported'
                    % (self, self.output_format)
                )

        # 1. Escape string parameters in the PERCENT output format
        if self.output_format == OutputFormats.PERCENT:
            # any % must be intended as literal and must be doubled
            query_string = query_string.replace('%', '%%')

        # 2. Replace the parameter with its dialect-specific representation
        query_string = re.sub(pattern, replace_parameter, query_string).strip()

        # 3. Un-escape remaining escaped colon params
        if self.output_format == OutputFormats.COLON:
            # replace \:word with :word because the colon-escape is no longer needed.
            query_string = re.sub(r"\\:(\w+)\b", r":\1", query_string)

        # 4. Build the parameter_values dict or list for use with the query
        if self.output_format in [OutputFormats.COLON, OutputFormats.PERCENT]:
            # parameter_values is a dict of fields
            parameter_values = {
                key: json.dumps(val) if isinstance(val, dict) else val
                for key, val in {field: data[field] for field in fields}.items()
            }
        elif self.output_format in [OutputFormats.QMARK, OutputFormats.NUMBERED]:
            parameter_values = [
                json.dumps(val) if isinstance(val, dict) else val
                for val in [data[field] for field in fields]
            ]
        else:
            raise ValueError(
                'Dialect %r output_format %r not supported' % (self, self.output_format)
            )

        if self == Dialect.ASYNCPG:
            return tuple([query_string] + parameter_values)
        else:
            return query_string, parameter_values


DEFAULT_DIALECT = Dialect.EMBEDDED
