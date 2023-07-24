from pathlib import Path

PATH = Path(__file__).absolute().parent

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
    "psycopg",
    "asyncpg",
    "databases",
    "sqlalchemy",
    "sqlite",
]

invalid_dialect_names = [None, "", "foo"]

test_databases = [
    # dialect, url
    ("sqlite", ":memory:"),
    ("sqlite", f"file://{PATH}/test.db"),
    ("psycopg", "postgresql://postgres:password@localhost:5432/testapp"),
]
