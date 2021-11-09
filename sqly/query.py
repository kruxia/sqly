import logging
from dataclasses import dataclass, field

from .dialect import DEFAULT_DIALECT
from .lib import walk_list

logger = logging.getLogger(__name__)


@dataclass
class Query:
    query: list = field(default_factory=list)
    dialect: str = DEFAULT_DIALECT

    def __post_init__(self):
        # allow the query to be a single string
        if isinstance(self.query, str):
            self.query = [self.query]

    def __str__(self):
        return ' '.join([str(q) for q in walk_list(self.query) if q]).strip()

    def render(self, data=None):
        """
        Render the query and its values for a given data input.
        """
        data = data or {}
        query_string = str(self)
        return self.dialect.render(query_string, data)

    @classmethod
    def fields(cls, data):
        return ', '.join(str(key) for key in data)

    @classmethod
    def params(cls, data):
        return ', '.join(f':{key}' for key in data)

    @classmethod
    def assigns(cls, data, excludes=None):
        excludes = excludes or []
        return ', '.join(f'{key}=:{key}' for key in data if key not in excludes)

    @classmethod
    def where(cls, data):
        return ' AND '.join(f'{key}=:{key}' for key in data)