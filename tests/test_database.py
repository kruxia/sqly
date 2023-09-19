# import json
import os

import pytest

from sqly.dialect import Dialect
from sqly import lib
from sqly.sql import SQL
from tests import fixtures


@pytest.mark.parametrize("dialect", [Dialect("sqlite"), "sqlite"])
def test_init_database_dialect(dialect):
    """
    SQL instance can be initialized with dialect as either Dialect or str
    """
    sql = SQL(dialect=dialect)
    assert isinstance(sql.dialect, Dialect)


@pytest.mark.parametrize("dialect_name,database_url", fixtures.test_databases)
def test_execute_query_ok(dialect_name, database_url):
    """
    - Executing a query on a connection does not commit that query.
    - but the changes are visible on the resulting cursor.
    """
    try:
        # connect to the database
        sql = SQL(dialect=dialect_name)
        adaptor = sql.dialect.adaptor()
        # if sql.dialect == Dialect.MYSQL:
        #     conn_info = json.loads(database_url)
        #     connection = adaptor.connect(**conn_info)
        # else:
        connection = lib.run(adaptor.connect(database_url))

        # execute a query that should have visible results
        cursor = lib.run(connection.execute("CREATE TABLE widgets (id int, sku varchar)"))

        print(f"{connection=}")
        widget = {"id": 1, "sku": "COG-01"}
        # - the following table exists (and using the cursor to execute is fine)
        lib.run(sql.execute(cursor, "INSERT INTO widgets (id, sku) VALUES (:id, :sku)", widget))

        print(f"{connection=}")
        # - the row is in the table
        row = lib.run(sql.select_one(cursor, "SELECT * from widgets WHERE id=:id", widget))
        assert row == widget

        # after we rollback, the table doesn't exist (NOTE: This might not work on all
        # databases, because not all have transactional DDL. )
        lib.run(connection.rollback())
        with pytest.raises(Exception):
            row = lib.run(sql.select_one(connection, "SELECT * from widgets WHERE id=:id", widget))
            # If the DDL wasn't transactional, the row still doesn't exist - is None
            assert row

    finally:
        # clean up the tables, if any
        try:
            lib.run(connection.execute("DROP TABLE widgets"))
            lib.run(connection.commit())
        except Exception:
            ...

        try:
            lib.run(connection.execute("DROP TABLE sqly_migrations"))
            lib.run(connection.commit())
        except Exception:
            ...

        # clean up database file if any
        db_file = database_url.split("file://")[-1] if "file://" in database_url else ""
        if os.path.exists(db_file):
            os.remove(db_file)


@pytest.mark.parametrize("dialect_name,database_url", fixtures.test_databases)
def test_execute_invalid_rollback(dialect_name, database_url):
    """
    If execution of a query fails, the connection is rolled back and ready for use.
    """
    try:
        # connect to the database
        sql = SQL(dialect=dialect_name)

        adaptor = sql.dialect.adaptor()
        # if sql.dialect == Dialect.MYSQL:
        #     conn_info = json.loads(database_url)
        #     connection = adaptor.connect(**conn_info)
        # else:
        connection = lib.run(adaptor.connect(database_url))

        # execute an invalid query
        insert_query = "INSERT INTO widgets (id, sku) VALUES (:id, :sku)"
        widget = {"id": 1, "sku": "COG-01"}
        # table widgets doesn't exist
        with pytest.raises(Exception):
            lib.run(sql.execute(connection, insert_query, widget))

        # the connection is ready for the next queries
        lib.run(sql.execute(connection, "CREATE TABLE widgets (id int, sku varchar)"))
        lib.run(connection.commit())
        lib.run(sql.execute(connection, insert_query, widget))
        rows = lib.gen(sql.select(connection, "SELECT * FROM widgets"))
        assert len(rows) == 1

        # and we can still rollback the connection (the insert)
        lib.run(connection.rollback())

        # TODO: work for async select
        # rows = list(lib.run(sql.select(connection, "SELECT * FROM widgets")))
        # assert len(rows) == 0

    finally:
        # clean up the tables, if any
        try:
            lib.run(connection.execute("DROP TABLE widgets"))
            lib.run(connection.commit())
        except Exception:
            ...

        try:
            lib.run(connection.execute("DROP TABLE sqly_migrations"))
            lib.run(connection.commit())
        except Exception:
            ...

        # clean up database file if any
        db_file = database_url.split("file://")[-1] if "file://" in database_url else ""
        if os.path.exists(db_file):
            os.remove(db_file)


@pytest.mark.parametrize("dialect_name,database_url", fixtures.test_databases)
def test_cursor_as_connection(dialect_name, database_url):
    """
    SQL queries can re-use a cursor during the same connection.
    """
    try:
        # connect to the database
        sql = SQL(dialect=dialect_name)
        adaptor = sql.dialect.adaptor()
        # if sql.dialect == Dialect.MYSQL:
        #     conn_info = json.loads(database_url)
        #     connection = adaptor.connect(**conn_info)
        # else:
        connection = lib.run(adaptor.connect(database_url))
        cursor = lib.run(sql.execute(connection, "CREATE TABLE WIDGETS (id int, sku varchar)"))
        lib.run(connection.commit())
        with pytest.raises(Exception, match="foo"):
            lib.run(sql.execute(cursor, "INSERT INTO foo VALUES (1, 2)"))
        lib.run(connection.rollback())

        widget = {"id": 1, "sku": "COG-01"}
        cursor2 = lib.run(sql.execute(cursor, "INSERT INTO widgets VALUES (:id, :sku)", widget))
        assert cursor2 == cursor
        record = lib.run(sql.select_one(cursor, "SELECT * FROM widgets"))
        assert record == widget

    finally:
        # clean up the tables, if any
        try:
            lib.run(connection.execute("DROP TABLE widgets"))
            lib.run(connection.commit())
        except Exception:
            ...

        try:
            lib.run(connection.execute("DROP TABLE sqly_migrations"))
            lib.run(connection.commit())
        except Exception:
            ...

        # clean up database file if any
        db_file = database_url.split("file://")[-1] if "file://" in database_url else ""
        if os.path.exists(db_file):
            os.remove(db_file)
