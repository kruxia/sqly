import logging
from pydantic import BaseModel, Field, validator

from .dialect import DEFAULT_DIALECT
from .lib import walk_list

logger = logging.getLogger(__name__)


class Query(BaseModel):
    query: list = Field(default_factory=list)
    dialect: str = DEFAULT_DIALECT

    @validator('query', pre=True)
    def convert_query(cls, value):
        # allow the query to be a single string
        if isinstance(value, str):
            value = [value]
        return value

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
    def filters(cls, data):
        return ' AND '.join(f'{key}=:{key}' for key in data)

    def __str__(self):
        return ' '.join([str(q) for q in walk_list(self.query) if q]).strip()

    def render(self, data=None):
        """
        Render the query and its values for a given data input.
        """
        data = data or {}
        query_string = str(self)
        return self.dialect.render(query_string, data)
