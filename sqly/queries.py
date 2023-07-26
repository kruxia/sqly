"""
A set of basic CRUD queries that make it easier to get started using SQLY and also
provide basic examples of using the library to construct queries.

The names of these queries are all caps (SELECT, etc.) to remind users that we are just
constructing SQL strings representing queries of the same names - SELECT, etc. are
capitalized in SQL. It also helps them to stand out in code, making it a little easier
to audit where in the codebase queries are being constructed.
"""
from typing import Iterable, Optional

from sqly.query import Q


def SELECT(
    relation: str,
    fields: Optional[Iterable] = None,
    filters: Optional[list[str]] = None,
    orderby: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> str:
    """
    Build a SELECT query with the following form:
    ```sql
    SELECT fields FROM relation
        [WHERE filters]
        [ORDER BY orderby]
        [LIMIT limit]
        [OFFSET offset]
    ```
    Arguments:
        relation (str): The name of the table or view from which to SELECT.
        fields (Iterable[str]): An iterable of field names to include in the SELECT.
        filters (Iterable[str]): An iterable of WHERE filters to apply.
        orderby (str): A string representing which fields to ORDER BY.
        limit (int): The LIMIT on the maximum number of records to SELECT.
        offset (int): The OFFSET to apply to the SELECT.

    Returns:
        sql (str): The string representing the SELECT query.
    """
    fields = fields or ["*"]
    query = [
        f"SELECT {Q.fields(fields)}",
        f"FROM {relation}",
    ]
    if filters:
        query.append(f"WHERE {' AND '.join(filters)}")
    if orderby:
        query.append(f"ORDER BY {orderby}")
    if limit:
        query.append(f"LIMIT {limit}")
    if offset:
        query.append(f"OFFSET {offset}")

    return " ".join(query)


def INSERT(relation: str, data: Iterable) -> str:
    """
    Build an INSERT query with the following form:
    ```sql
    INSERT INTO relation (fields(data))
    VALUES (params(data))
    ```
    Arguments:
        relation (str): The name of the table to INSERT INTO.
        data (Iterable): An iterable representing field names to insert.

    Returns:
        sql (str): The string representing the INSERT query.
    """
    query = [
        f"INSERT INTO {relation}",
        f"({Q.fields(data)})",
        f"VALUES ({Q.params(data)})",
    ]
    return " ".join(query)


def UPDATE(relation: str, data: Iterable, filters: Iterable[str]) -> str:
    """
    Build an UPDATE query with the following form:
    ```sql
    UPDATE relation
    SET (assigns(data))
    WHERE (filters)
    ```
    Arguments:
        relation (str): The name of the table to UPDATE.
        data (Iterable): An iterable representing field names to update.
        filters (Iterable): An iterable of strings represent WHERE filters. At least one
            filter is required.

    Returns:
        sql (str): The string representing the SELECT query.
    """
    query = [
        f"UPDATE {relation}",
        f"SET {Q.assigns(data)}",
        f"WHERE {' AND '.join(filters)}",
    ]
    return " ".join(query)


def DELETE(relation: str, filters: Iterable[str]) -> str:
    """
    Build a DELETE query with the following form:
    ```sql
    DELETE FROM relation
    WHERE (filters)
    ```
    Arguments:
        relation (str): The name of the table to DELETE FROM.
        filters (Iterable): An iterable of strings represent WHERE filters. At least one
            filter is required.

    Returns:
        sql (str): The string representing the SELECT query.
    """
    query = [
        f"DELETE FROM {relation}",
        f"WHERE {' AND '.join(filters)}",
    ]
    return " ".join(query)
