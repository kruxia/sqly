import pytest

from sqly.database import Database
from sqly.dialect import Dialect


@pytest.mark.parametrize("dialect", [Dialect("sqlite"), "sqlite"])
def test_init_database_dialect(dialect):
    """
    Database can be initialized with dialect as either Dialect or str
    """
    database = Database(dialect=dialect)
    assert isinstance(database.dialect, Dialect)


@pytest.mark.parametrize(
    "dialect_name,database_url",
    [
        ("sqlite", ":memory:"),
    ],
)
def test_execute_query_ok(dialect_name, database_url):
    """
    - Executing a query on a connection does not commit that query.
    - but the changes are visible on the resulting cursor.
    """
    # connect to the database
    database = Database(dialect=dialect_name)
    adaptor = database.dialect.load_adaptor()
    connection = adaptor.connect(database_url)

    # execute a query that should have visible results
    cursor = connection.execute("CREATE TABLE widgets (id int, sku varchar)")
    print(f"{connection=}")
    widget = {"id": 1, "sku": "COG-01"}
    # - the following table exists (and using the cursor to execute is fine)
    database.execute(cursor, "INSERT INTO widgets (id, sku) VALUES (:id, :sku)", widget)
    print(f"{connection=}")
    # - the row is in the table
    row = next(database.query(cursor, "SELECT * from widgets WHERE id=:id", widget))
    assert row == widget

    # after we rollback, the row doesn't exist (NOTE: This might not work on all
    # databases, because not all have transactional DDL. )
    connection.rollback()
    with pytest.raises(StopIteration):
        row = next(
            database.query(connection, "SELECT * from widgets WHERE id=:id", widget)
        )


@pytest.mark.parametrize(
    "dialect_name,database_url",
    [
        ("sqlite", ":memory:"),
    ],
)
def test_execute_invalid_rollback(dialect_name, database_url):
    """
    If execution of a query fails, the connection is rolled back and ready for use.
    """
    # connect to the database
    database = Database(dialect=dialect_name)
    adaptor = database.dialect.load_adaptor()
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
