from pydantic import BaseModel, Field, validator

from .lib import walk


class Query(BaseModel):
    query: list = Field(default_factory=list)

    @validator("query", pre=True)
    def convert_query(cls, value):
        # allow the query to be a single string
        if isinstance(value, str):
            value = [value]
        return value

    @classmethod
    def fields(cls, data, excludes=None, prefix=None):
        return ", ".join(
            f"{prefix+'.' if prefix else ''}{key}"
            for key in data
            if key not in (excludes or [])
        )

    @classmethod
    def params(cls, data, excludes=None):
        return ", ".join(f":{key}" for key in data if key not in (excludes or []))

    @classmethod
    def assigns(cls, data, excludes=None, prefix=None):
        return ", ".join(
            f"{prefix+'.' if prefix else ''}{key}=:{key}"
            for key in data
            if key not in (excludes or [])
        )

    @classmethod
    def filters(cls, data, excludes=None, prefix=None):
        return " AND ".join(
            f"{prefix+'.' if prefix else ''}{key}=:{key}"
            for key in data
            if key not in (excludes or [])
        )

    def __str__(self):
        return " ".join([str(q) for q in walk(self.query) if q]).strip()
