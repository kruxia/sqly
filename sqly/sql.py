import json
import re
from dataclasses import dataclass

from .dialect import Dialect, OutputFormat
from .lib import walk


@dataclass
class SQL:
    """
    Create and render SQL queries with a given dialect. All queries are rendered
    according to that dialect.
    """

    dialect: Dialect

    def __post_init__(self):
        if isinstance(self.dialect, str):
            self.dialect = Dialect(self.dialect)

    def render(self, query, data=None):
        """
        Render a query string and its parameters for this SQL dialect.

        Arguments:

        * query: a string or iterator of strings.
        * data: a keyword dict used to render the query.

        Returns a 2-tuple:

        1. the rendered query string.
        2. depends on the output format:
            - positional output formats (QMARK, NUMBERED) return a tuple of values
            - named output formats (NAMED, PYFORMAT) return a dict
        """
        # ordered list of fields for positional outputs (closure for replace_parameter)
        fields = []

        def replace_parameter(match):
            field = match.group(1)

            # Build the ordered fields list
            if self.dialect.output_format.is_positional or field not in fields:
                fields.append(field)

            # Return the field formatted for the output type
            if self.dialect.output_format == OutputFormat.NAMED:
                return f":{field}"
            elif self.dialect.output_format == OutputFormat.PYFORMAT:
                return f"%({field})s"
            elif self.dialect.output_format == OutputFormat.QMARK:
                return "?"
            elif self.dialect.output_format == OutputFormat.NUMBERED:
                return f"${len(fields)}"
            else:
                raise ValueError(
                    "Dialect %r output_format %r not supported"
                    % (self.dialect, self.dialect.output_format)
                )

        # 1. Convert query to a string
        if isinstance(query, str) or hasattr(query, "__str__"):
            query_str = str(query)
        elif hasattr(query, "__iter__"):
            query_str = "\n".join(str(q) for q in walk(query))
        else:
            raise ValueError(f"Query has unsupported type: {type(query)}")

        # 2. Escape string parameters in the PYFORMAT output format
        if self.dialect.output_format == OutputFormat.PYFORMAT:
            # any % must be intended as literal and must be doubled
            query_str = query_str.replace("%", "%%")

        # 3. Replace the parameter with its dialect-specific representation
        pattern = r"(?<!\\):(\w+)\b"  # colon + word not preceded by a backslash
        query_str = re.sub(pattern, replace_parameter, query_str).strip()

        # 4. Un-escape remaining escaped colon params
        if self.dialect.output_format == OutputFormat.NAMED:
            # replace \:word with :word because the colon-escape is no longer needed.
            query_str = re.sub(r"\\:(\w+)\b", r":\1", query_str)

        # 5. Build the parameter_values dict or list for use with the query
        if self.dialect.output_format.is_positional:
            # parameter_values is a list of values
            parameter_values = [
                json.dumps(val) if isinstance(val, dict) else val
                for val in [data[field] for field in fields]
            ]
        else:
            # parameter_values is a dict of key:value fields
            parameter_values = {
                key: json.dumps(val) if isinstance(val, (dict, list, tuple)) else val
                for key, val in {field: data[field] for field in fields}.items()
            }

        # 6. Return a tuple formatted for this Dialect
        if self.dialect == Dialect.ASYNCPG:
            # asyncpg expects the parameters in a tuple following the query string.
            return tuple([query_str] + parameter_values)
        else:
            # other dialects expect the parameters in the second tuple item.
            return (query_str, parameter_values)
