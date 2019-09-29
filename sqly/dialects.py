import json
import re
from enum import Enum


class OutputFormats(Enum):
    COLON_PARAM = ':field'
    STR_PARAM = '%(field)s'
    QMARK = '?'
    DOLLAR_POS = '$i'


class Dialects(Enum):
    # the value of each Dialect represents the syntax that it uses for query parameters
    EMBEDDED = 'embedded'
    MYSQL = 'mysql'
    SQLITE = 'sqlite'
    POSTGRES = 'postgres'
    ASYNCPG = 'asyncpg'
    PSYCOPG2 = 'psycopg2'
    SQLALCHEMY = 'sqlalchemy'

    @property
    def supports_returning(self):
        return self in [self.POSTGRES, self.PSYCOPG2, self.ASYNCPG]

    @property
    def output_format(self):
        return {
            self.EMBEDDED: OutputFormats.COLON_PARAM,
            self.SQLALCHEMY: OutputFormats.COLON_PARAM,
            self.MYSQL: OutputFormats.STR_PARAM,
            self.PSYCOPG2: OutputFormats.STR_PARAM,
            self.SQLITE: OutputFormats.QMARK,
            self.POSTGRES: OutputFormats.DOLLAR_POS,
            self.ASYNCPG: OutputFormats.DOLLAR_POS,
        }[self]

    def render(self, query_string, data):
        """render a query_string and its parameter values for this dialect."""
        pattern = r"(?<!\\):(\w+)\b"
        fields = []  # ordered list of fields for positional outputs

        def replace_parameter(match):
            field = match.group(1)

            # Build the ordered fields list for positional outputs
            if (
                self.output_format in [OutputFormats.QMARK, OutputFormats.DOLLAR_POS]
                or field not in fields
            ):
                fields.append(field)

            # Return the field formatted for the output type
            if self.output_format == OutputFormats.COLON_PARAM:
                return f":{field}"
            elif self.output_format == OutputFormats.STR_PARAM:
                return f"%({field})s"
            elif self.output_format == OutputFormats.QMARK:
                return "?"
            elif self.output_format == OutputFormats.DOLLAR_POS:
                return f"${len(fields)}"
            else:
                raise ValueError(
                    'Dialect %r output_format %r not supported'
                    % (self, self.output_format)
                )

        # 1. Escape string parameters in the STR_PARAM output format
        if self.output_format == OutputFormats.STR_PARAM:
            # any % must be intended as literal and must be doubled
            query_string = query_string.replace('%', '%%')

        # 2. Replace the parameter with its dialect-specific representation
        rendered_query_string = re.sub(pattern, replace_parameter, query_string).strip()

        # 3. Un-escape remaining escaped colon params
        if self.output_format == OutputFormats.COLON_PARAM:
            # replace \:word with :word because the colon-escape is no longer needed.
            rendered_query_string = re.sub(r"\\:(\w+)\b", r":\1", rendered_query_string)

        # 4. Build the parameter_values dict or list for use with the query
        if self.output_format in [OutputFormats.COLON_PARAM, OutputFormats.STR_PARAM]:
            # parameter_values is a dict of fields
            parameter_values = {
                key: json.dumps(val) if isinstance(val, dict) else val
                for key, val in {field: data[field] for field in fields}.items()
            }
        elif self.output_format in [OutputFormats.QMARK, OutputFormats.DOLLAR_POS]:
            parameter_values = [
                json.dumps(val) if isinstance(val, dict) else val
                for val in [data[field] for field in fields]
            ]
        else:
            raise ValueError(
                'Dialect %r output_format %r not supported' % (self, self.output_format)
            )

        if self == Dialects.ASYNCPG:
            return tuple([rendered_query_string] + parameter_values)
        else:
            return rendered_query_string, parameter_values


DEFAULT_DIALECT = Dialects.EMBEDDED
