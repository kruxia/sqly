import pytest

from sqly import SQL, Dialect, queries
from tests import fixtures


def get_query_params(sql, q, data=None):
    result = sql.render(q, data or {})
    if sql.dialect == Dialect.ASYNCPG:
        query = result[0]
        params = result[1:]
    else:
        query, params = result

    return query, params


@pytest.mark.parametrize("dialect_name", fixtures.valid_dialect_names)
def test_sql_init(dialect_name):
    sql = SQL(dialect=dialect_name)
    assert isinstance(sql.dialect, Dialect)
    assert sql.dialect.value == dialect_name


@pytest.mark.parametrize("dialect_name", fixtures.invalid_dialect_names)
def test_sql_init_invalid(dialect_name):
    with pytest.raises(Exception):
        sql = SQL(dialect=dialect_name)
        print(f"{sql=}")


@pytest.mark.parametrize("dialect_name", fixtures.valid_dialect_names)
def test_sql_render(dialect_name):
    print(dialect_name)
    data = {"a": 1, "b": 2, "c": 3}
    filters = ["a = :a"]
    sql = SQL(dialect=dialect_name)
    q = queries.UPDATE("the_table", data, filters)
    print(q)
    query, params = get_query_params(sql, q, data)
    print(query, params)

    if sql.dialect.output_format.is_keyed:
        assert len(params) == len(data)
    else:
        assert len(params) == len(data) + len(filters)


@pytest.mark.parametrize("dialect_name", fixtures.valid_dialect_names)
def test_sql_render_nested_query(dialect_name):
    sql = SQL(dialect=dialect_name)
    q = ["select", ["*", ["from", "tablename"]]]
    query, params = get_query_params(sql, q)
    assert isinstance(query, str)
    assert not params


@pytest.mark.parametrize("dialect_name", fixtures.valid_dialect_names)
def test_sql_render_invalid_query_type(dialect_name):
    """
    A query that is not a string or an iterator raises a ValueError
    """

    class InvalidQuery:
        ...

    sql = SQL(dialect=dialect_name)
    query = InvalidQuery()
    with pytest.raises(ValueError):
        sql.render(query)
