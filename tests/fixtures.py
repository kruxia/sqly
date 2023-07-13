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
