class Q:
    """
    Convenience methods for building dynamic queries. Examples:

    >>> d = {"name": "Cheeseshop"}
    >>> f"INSERT INTO tablename ({Q.fields(d)}) VALUES ({Q.params(d)})"
    'INSERT INTO tablename (name) VALUES (:name)'
    >>> f"SELECT ({Q.fields(d)}) FROM tablename WHERE {Q.filters(d)}"
    'SELECT (name) FROM tablename WHERE name = :name'
    >>> d["id"] = 1
    >>> " ".join(
    ...     "UPDATE tablename SET",
    ...     Q.assigns(d, excl=['id']),
    ...     "WHERE",
    ...     Q.filters(d, incl=['id'])
    ... )
    'UPDATE tablename SET name = :name WHERE id = :id'
    >>> f"DELETE FROM tablename WHERE {Q.filters(d, incl=['name'])}"
    'DELETE FROM tablename WHERE name = :name'
    """

    @classmethod
    def fields(cls, data, incl=None, excl=None, pre=None) -> str:
        """
        Render a comma-separated list of field names from the given data. Use: E.g., for
        dynamically specifying SELECT or INSERT field lists.

        * data = the data from which to use the field names.
        * excl = a list / set of keys to exclude.
        * pre = a tablename prefix to use with each field name.
        """
        return ", ".join(
            f"{pre+'.' if pre else ''}{key}"
            for key in cls.keys(data, incl=incl, excl=excl)
        )

    @classmethod
    def params(cls, data, incl=None, excl=None) -> str:
        """
        Render a comma-separated list of parameters from the given data. Use: E.g.,
        dynamically specifying INSERT parameter lists.

        * data = the data from which to use the field names.
        * excl = a list / set of keys to exclude.
        """
        return ", ".join(f":{key}" for key in cls.keys(data, incl=incl, excl=excl))

    @classmethod
    def assigns(cls, data, incl=None, excl=None, pre=None, op="=", join=",") -> str:
        """
        Render a comma-separated list of field to parameter assignments from the given
        data. Use: E.g., for dynamically specifying UPDATE field lists.

        * data = the data from which to use the field names.
        * excl = a list / set of keys to exclude.
        * pre = a tablename prefix to use with each field name.
        * op = the operator to use in the assign (default `=`).
        * join = the operator to join phrases by (default `,`)
        """
        return f" {join} ".join(
            f"{pre+'.' if pre else ''}{key} {op} :{key}"
            for key in cls.keys(data, incl=incl, excl=excl)
        )

    @classmethod
    def filters(cls, data, incl=None, excl=None, pre=None, op="=", join="AND") -> str:
        return cls.assigns(data, incl=incl, excl=excl, pre=pre, op=op, join=join)

    @classmethod
    def filter(cls, key, val=None, op="="):
        return f"{key} {op} {val or ':' + key}"

    @classmethod
    def keys(cls, data, incl=None, excl=None) -> list:
        return (
            incl
            if incl
            else [key for key in data if key not in excl]
            if excl
            else list(data)
        )

    @classmethod
    def select(cls, relation, fields=None, filters=None, orderby=None, limit=None):
        fields = fields or ["*"]
        filters = filters or []
        query = [f"SELECT {cls.fields(fields)} FROM {relation}"]
        if filters:
            query.append(f"WHERE {cls.filters(filters)}")
        if orderby:
            query.append(f"ORDER BY {orderby}")
        if limit:   
            query.append(f"LIMIT {limit}")
        return ' '.join(query)
        
    @classmethod
    def insert(cls, relation, data):
        query = [
            f"INSERT INTO {relation}",
            f"({cls.fields(data)})",
            f"VALUES ({cls.params(data)})"
        ]
        return ' '.join(query)
    
    @classmethod
    def update(cls, relation, data, filters):
        query = [
            f"UPDATE {relation} SET {cls.assigns(data)}",
            f"WHERE {cls.filters(filters)}"
        ]
        return ' '.join(query)

    @classmethod
    def delete(cls, relation, filters):
        query = [
            f"DELETE FROM {relation}",
            f"WHERE {cls.filters(filters)}",
        ]
        return ' '.join(query)
    
    