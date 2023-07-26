from typing import Iterable, Optional


class Q:
    """
    Convenience methods for building dynamic queries.

    Examples:
        >>> d = {"name": "Cheeseshop"}
        >>> f"INSERT INTO tablename ({Q.fields(d)}) VALUES ({Q.params(d)})"
        'INSERT INTO tablename (name) VALUES (:name)'
        >>> f"SELECT ({Q.fields(d)}) FROM tablename WHERE {Q.filter('name')}"
        'SELECT (name) FROM tablename WHERE name = :name'
        >>> " ".join([
        ...     "UPDATE tablename SET",
        ...     Q.assigns(['name']),
        ...     "WHERE",
        ...     Q.filter('id'),
        ... ])
        'UPDATE tablename SET name = :name WHERE id = :id'
        >>> f"DELETE FROM tablename WHERE {Q.filter('name')}"
        'DELETE FROM tablename WHERE name = :name'
    """

    @classmethod
    def keys(cls, fields: Iterable) -> list:
        """
        Return a list of field names from the given iterator.

        Arguments:
            fields (Iterable): An iterable of field names. (Can be a Mapping with field
                names as keys.)

        Returns:
            (list): A list of field names

        Examples:
            >>> Q.keys({'id': 1, 'name': 'Mark'})
            ['id', 'name']
        """
        return list(fields)

    @classmethod
    def fields(cls, fields: Iterable) -> str:
        """
        Render a comma-separated string of field names from the given fields. Use: E.g.,
        for dynamically specifying SELECT or INSERT field lists.

        Arguments:
            fields (Iterable): An iterable of field names. (Can be a Mapping with field
                names as keys.)

        Returns:
            (str): A comma-separated string of field names

        Examples:
            >>> Q.fields({'id': 1, 'name': 'Mark'})
            'id, name'
        """
        return ", ".join(fields)

    @classmethod
    def params(cls, fields: Iterable) -> str:
        """
        Render a comma-separated list of parameters from the given fields. Use: E.g.,
        dynamically specifying INSERT parameter lists.

        Arguments:
            fields (Iterable): An iterable of field names. (Can be a Mapping with field
                names as keys.)

        Returns:
            (str): A comma-separated string of field names

        Examples:
            >>> Q.params({'id': 1, 'name': 'Mark'})
            ':id, :name'
        """
        return ", ".join(f":{key}" for key in cls.keys(fields))

    @classmethod
    def assigns(cls, fields: Iterable) -> str:
        """
        Render a comma-separated list of assignments from the given fields. Use: E.g.,
        for dynamically specifying UPDATE field lists.

        Arguments:
            fields (Iterable): An iterable of field names. (Can be a Mapping with field
                names as keys.)

        Returns:
            (str): A comma-separated string of field `key = :key` assignments

        Examples:
            >>> Q.assigns({'id': 1, 'name': 'Mark'})
            'id = :id, name = :name'
        """
        return ", ".join(f"{key} = :{key}" for key in cls.keys(fields))

    @classmethod
    def filter(cls, field: str, *, op: Optional[str] = "="):
        """
        Render a filter from the given field, optional operator, and optional value.

        Arguments:
            field (str): The name of the field.
            op (str): The operator to use in the filter.

        Returns:
            (str): A comma-separated string of field names

        Examples:
            >>> Q.filter('id', op='>')
            'id > :id'
        """
        return f"{field} {op} :{field}"
