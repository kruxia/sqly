import asyncio
import inspect
import os
from pathlib import Path

# from textwrap import dedent

PATH = Path(__file__).absolute().parent
POSTGRESQL_URL = os.environ["POSTGRESQL_URL"]


fields = [
    # list
    (["a", "b", "c"]),
    # set
    ({"a", "b", "c"}),
    # dict
    ({"a": 1, "b": 2, "c": "%three%"}),
]

field_filter_ops = {"b": ">", "c": "like"}

valid_dialect_names = [
    "sqlite",
    "asyncpg",
    "psycopg",
    # "databases",
    # "sqlalchemy",
]

invalid_dialect_names = [None, "", "foo"]

test_databases = [
    # dialect, url
    ("sqlite", ":memory:"),
    ("sqlite", f"file://{PATH}/test.db"),
    ("psycopg", POSTGRESQL_URL),
    # ("asyncpg", POSTGRESQL_URL),
    # (
    #     "mysql",
    #     dedent(
    #         """
    #         {"host": "mysql", "port": "3306", "user": "testapp",
    #         "password": "password", "database": "testapp"}
    #         """
    #     ),
    # ),
]
