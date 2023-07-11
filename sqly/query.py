from typing import Any, Iterable, Optional


class Q:
    """
    Convenience methods for building dynamic queries. Examples:

    >>> d = {"name": "Cheeseshop"}
    >>> f"INSERT INTO tablename ({Q.fields(d)}) VALUES ({Q.params(d)})"
    'INSERT INTO tablename (name) VALUES (:name)'
    >>> f"SELECT ({Q.fields(d)}) FROM tablename WHERE {Q.filters(d)}"
    'SELECT (name) FROM tablename WHERE name = :name'
    >>> " ".join(
    ...     "UPDATE tablename SET",
    ...     Q.assigns(['name']),
    ...     "WHERE",
    ...     Q.filter('id'),
    ... )
    'UPDATE tablename SET name = :name WHERE id = :id'
    >>> f"DELETE FROM tablename WHERE {Q.filters(d, incl=['name'])}"
    'DELETE FROM tablename WHERE name = :name'
    """

    @classmethod
    def keys(cls, fields: Iterable) -> list:
        return [key for key in fields]

    @classmethod
    def fields(cls, fields: Iterable) -> str:
        """
        Render a comma-separated list of field names from the given fields. Use: E.g.,
        for dynamically specifying SELECT or INSERT field lists.
        """
        return ", ".join(cls.keys(fields))

    @classmethod
    def params(cls, fields: Iterable) -> str:
        """
        Render a comma-separated list of parameters from the given fields. Use: E.g.,
        dynamically specifying INSERT parameter lists.
        """
        return ", ".join(f":{key}" for key in cls.keys(fields))

    @classmethod
    def assigns(cls, fields: Iterable) -> str:
        """
        Render a comma-separated list of assignments from the given fields. Use: E.g.,
        for dynamically specifying UPDATE field lists.
        """
        return ", ".join(f"{key} = :{key}" for key in cls.keys(fields))

    @classmethod
    def filter(cls, key: str, *, op: Optional[str] = "=", val: Optional[Any] = None):
        """
        Render a filter from the given field key, optional operator, and optional value.
        """
        return f"{key} {op} {val or ':' + key}"
