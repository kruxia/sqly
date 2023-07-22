"""
A set of basic CRUD queries that make it easier to get started using SQLY and also
provide basic examples of using the library to construct queries.

The names of these queries are all caps (SELECT, etc.) to remind users that we are just
constructing SQL strings representing queries of the same names - SELECT, etc. are
capitalized in SQL. It also helps them to stand out in code, making it a little easier
to audit where in the codebase queries are being constructed.
"""
from typing import Iterable

from sqly.query import Q


def SELECT(
    relation: str, fields=None, filters=None, orderby=None, limit=None, offset=None
) -> str:
    """
    SELECT fields
        FROM relation
        [WHERE filters]
        [ORDER BY orderby]
        [LIMIT limit]
        [OFFSET offset].
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
    return query


def INSERT(relation: str, data: Iterable) -> str:
    """
    INSERT INTO relation
    (fields(data))
    VALUES (params(data))
    """
    query = [
        f"INSERT INTO {relation}",
        f"({Q.fields(data)})",
        f"VALUES ({Q.params(data)})",
    ]
    return " ".join(query)


def UPDATE(relation: str, data: Iterable, filters: Iterable[str]) -> str:
    """
    UPDATE relation
    SET (assigns(data))
    WHERE (filters)
    """
    query = [
        f"UPDATE {relation}",
        f"SET {Q.assigns(data)}",
        f"WHERE {' AND '.join(filters)}",
    ]
    return " ".join(query)


def DELETE(relation: str, filters: Iterable[str]) -> str:
    """
    DELETE FROM relation
    WHERE (filters)
    """
    query = [
        f"DELETE FROM {relation}",
        f"WHERE {' AND '.join(filters)}",
    ]
    return " ".join(query)
