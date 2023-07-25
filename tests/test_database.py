# import json
import os

import pytest

from sqly.database import Database
from sqly.dialect import Dialect
from tests import fixtures


@pytest.mark.parametrize("dialect", [Dialect("sqlite"), "sqlite"])
def test_init_database_dialect(dialect):
    """
    Database can be initialized with dialect as either Dialect or str
    """
    database = Database(dialect=dialect)
    assert isinstance(database.dialect, Dialect)


@pytest.mark.parametrize("dialect_name,database_url", fixtures.test_databases)
def test_execute_query_ok(dialect_name, database_url):
    """
    - Executing a query on a connection does not commit that query.
    - but the changes are visible on the resulting cursor.
    """
    try:
        # connect to the database
        database = Database(dialect=dialect_name)
        adaptor = database.dialect.load_adaptor()
        # if database.dialect == Dialect.MYSQL:
        #     conn_info = json.loads(database_url)
        #     connection = adaptor.connect(**conn_info)
        # else:
        connection = adaptor.connect(database_url)

        # execute a query that should have visible results
        cursor = connection.execute("CREATE TABLE widgets (id int, sku varchar)")
        print(f"{connection=}")
        widget = {"id": 1, "sku": "COG-01"}
        # - the following table exists (and using the cursor to execute is fine)
        database.execute(
            cursor, "INSERT INTO widgets (id, sku) VALUES (:id, :sku)", widget
        )
        print(f"{connection=}")
        # - the row is in the table
        row = next(database.query(cursor, "SELECT * from widgets WHERE id=:id", widget))
        assert row == widget

        # after we rollback, the row doesn't exist (NOTE: This might not work on all
        # databases, because not all have transactional DDL. )
        connection.rollback()
        with pytest.raises(Exception):
            row = next(
                database.query(connection, "SELECT * from widgets WHERE id=:id", widget)
            )

    finally:
        # clean up the tables, if any
        try:
            connection.execute("DROP TABLE widgets")
            connection.commit()
        except Exception:
            ...

        try:
            connection.execute("DROP TABLE sqly_migrations")
            connection.commit()
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
        database = Database(dialect=dialect_name)
        adaptor = database.dialect.load_adaptor()
        # if database.dialect == Dialect.MYSQL:
        #     conn_info = json.loads(database_url)
        #     connection = adaptor.connect(**conn_info)
        # else:
        connection = adaptor.connect(database_url)

        # execute an invalid query
        insert_query = "INSERT INTO widgets (id, sku) VALUES (:id, :sku)"
        widget = {"id": 1, "sku": "COG-01"}
        with pytest.raises(Exception):
            # table widgets doesn't exist
            database.execute(connection, insert_query, widget)

        # the connection is ready for the next queries
        database.execute(connection, "CREATE TABLE widgets (id int, sku varchar)")
        connection.commit()
        database.execute(connection, insert_query, widget)
        rows = list(database.query(connection, "SELECT * FROM widgets"))
        assert len(rows) == 1

        # and we can still rollback the connection (the insert)
        connection.rollback()
        rows = list(database.query(connection, "SELECT * FROM widgets"))
        assert len(rows) == 0

    finally:
        # clean up the tables, if any
        try:
            connection.execute("DROP TABLE widgets")
            connection.commit()
        except Exception:
            ...

        try:
            connection.execute("DROP TABLE sqly_migrations")
            connection.commit()
        except Exception:
            ...

        # clean up database file if any
        db_file = database_url.split("file://")[-1] if "file://" in database_url else ""
        if os.path.exists(db_file):
            os.remove(db_file)
