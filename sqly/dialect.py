import json
import re
from enum import Enum
from importlib import import_module


class OutputFormats(Enum):
    COLON = ':field'
    PERCENT = '%(field)s'
    QMARK = '?'
    NUMBERED = '$i'

    @property
    def is_positional(self):
        return self in [self.QMARK, self.NUMBERED]


class Dialect(Enum):
    # the value of each Dialect represents the syntax that it uses for query parameters
    ASYNCPG = 'asyncpg'
    PSYCOPG2 = 'psycopg2'
    PYODBC = 'pyodbc'
    SQLALCHEMY = 'sqlalchemy'
    SQLITE = 'sqlite'

    @property
    def is_async(self):
        return self in [self.ASYNCPG]

    @property
    def supports_returning(self):
        return self in [self.PSYCOPG2, self.ASYNCPG]

    @property
    def output_format(self):
        return {
            self.ASYNCPG: OutputFormats.NUMBERED,
            self.PSYCOPG2: OutputFormats.PERCENT,
            self.PYODBC: OutputFormats.QMARK,
            self.SQLALCHEMY: OutputFormats.PERCENT,
            self.SQLITE: OutputFormats.QMARK,
        }[self]

    @property
    def adaptor(self):
        return {
            self.ASYNCPG: 'asyncpg',
            self.PSYCOPG2: 'psycopg2',
            self.PYODBC: 'pyodbc',
            self.SQLALCHEMY: 'sqlalchemy',
            self.SQLITE: 'sqlite3',
        }[self]

    def load_adaptor(self):
        return import_module(self.adaptor)

    def render(self, query_string, data):
        """
        Render a query_string and its parameters for this dialect.

        Returns 2-tuple:

        * first item is the rendered query string.
        * second item depends on the output format:
            - positional output formats return a tuple of values
            - non-positional (named) output formats return a dict
        """
        pattern = r"(?<!\\):(\w+)\b"  # colon + word not preceded by a backslash
        fields = []  # ordered list of fields for positional outputs (closure)

        def replace_parameter(match):
            field = match.group(1)

            # Build the ordered fields list for positional outputs (fields in closure)
            if self.output_format.is_positional or field not in fields:
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
        if self.output_format.is_named:
            parameter_values = [
                json.dumps(val) if isinstance(val, dict) else val
                for val in [data[field] for field in fields]
            ]
        else:
            # parameter_values is a dict of fields
            parameter_values = {
                key: json.dumps(val) if isinstance(val, dict) else val
                for key, val in {field: data[field] for field in fields}.items()
            }

        if self == Dialect.ASYNCPG:
            # asyncpg expects the parameters to be the second & following tuple items.
            return tuple([query_string] + parameter_values)
        else:
            # other dialects expect the parameters in the second tuple item.
            return (query_string, parameter_values)
