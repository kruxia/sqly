import json
import os
from glob import glob
from pathlib import Path

import pytest

import sqly
from sqly import Dialect, migration
from tests import fixtures

package_path = Path(os.path.abspath(sqly.__file__)).parent.parent
EXISTING_APPS = ["sqly", "testapp"]
EXISTING_MIGRATION_PATHS = [
    Path(p)
    for app in EXISTING_APPS
    for p in glob(str(package_path / app / "migrations" / "*.yaml"))
]
EXISTING_MIGRATION_KEYS = [
    f"{path.parent.parent.stem}:{path.stem}" for path in EXISTING_MIGRATION_PATHS
]


@pytest.mark.parametrize("app", ["sqly", "testapp"])
def test_app_migrations_path(app):
    print(app)
    path = migration.app_migrations_path(app)
    print(path)
    assert isinstance(path, Path)
    assert str(path).endswith("migrations")


@pytest.mark.parametrize("app", ["NONESUCH"])
def test_app_migrations_path_nonexistent(app):
    print(app)
    with pytest.raises(Exception):
        migration.app_migrations_path(app)


@pytest.mark.parametrize(
    "item",
    [
        {"app": "testapp"},
        {"app": "testapp", "name": "foo"},
        {"app": "testapp", "ts": 20230712231500000},
        {"app": "testapp", "depends": ""},
        {"app": "testapp", "depends": EXISTING_MIGRATION_KEYS},
    ],
)
def test_migration_init(item):
    print(item)
    m = migration.Migration(**item)
    print(m)
    assert m.app == item["app"]
    assert isinstance(m.ts, int)
    assert m.name == item.get("name", "")
    assert m.depends == list(item.get("depends") or [])
    assert m.applied == item.get("applied", None)
    assert m.doc == item.get("doc", None)
    assert m.up == item.get("up", None)
    assert m.dn == item.get("dn", None)
    r = repr(m)
    assert "key=" in r
    assert m.key in r
    assert isinstance(hash(m), int)
    d = m.dict()
    assert set(d.keys()) == set(m.__dataclass_fields__.keys())
    assert str(m.ts) in m.filename


def test_migration_init_json_depends():
    depends = json.dumps(EXISTING_MIGRATION_KEYS)
    item = {"app": "testapp", "depends": depends}
    m = migration.Migration(**item)
    assert m.depends == json.loads(depends)


@pytest.mark.parametrize("key", EXISTING_MIGRATION_KEYS)
def test_migration_key_load(key):
    key_filepath = migration.Migration.key_filepath(key)
    print(f"{key_filepath=}")
    assert os.path.exists(key_filepath)
    m = migration.Migration.key_load(key)
    app, ts_name = key.split(":")
    ts, name = ts_name.split("_")
    assert m.app == app
    assert m.ts == int(ts)
    assert m.name == name
    assert m.up and m.dn


@pytest.mark.parametrize("app", EXISTING_APPS)
def test_app_migrations(app):
    migrations = migration.Migration.app_migrations(app, include_depends=False)
    assert list(migrations.keys()) == [
        key for key in EXISTING_MIGRATION_KEYS if key.split(":")[0] == app
    ]


def test_app_migrations_include_depends():
    m = migration.Migration(app="testapp", depends=EXISTING_MIGRATION_KEYS)
    filepath, _ = m.save()
    migrations = migration.Migration.app_migrations("testapp", include_depends=True)
    os.remove(filepath)
    assert set(migrations) == set(EXISTING_MIGRATION_KEYS) | {m.key}


def test_all_migrations():
    migrations = migration.Migration.all_migrations(*EXISTING_APPS)
    assert list(migrations) == EXISTING_MIGRATION_KEYS


def test_migration_create():
    m = migration.Migration.create("testapp")
    assert m.depends == EXISTING_MIGRATION_KEYS
    assert m.name == ""


@pytest.mark.parametrize("dialect_name", fixtures.valid_dialect_names)
def test_migration_insert_query(dialect_name):
    m = migration.Migration.create("testapp")
    dialect = Dialect(dialect_name)
    result = m.insert_query(dialect)
    print(dialect, result)
    query = result[0]
    for key in ["app", "ts", "name", "depends"]:
        assert key in query


@pytest.mark.parametrize("dialect_name", fixtures.valid_dialect_names)
def test_migration_delete_query(dialect_name):
    m = migration.Migration.create("testapp")
    dialect = Dialect(dialect_name)
    result = m.delete_query(dialect)
    print(dialect, result)
    query = result[0]
    for key in ["app", "ts", "name"]:
        assert f"{key} = " in query


@pytest.mark.parametrize("dialect_name,database_url", fixtures.test_databases)
def test_migration_migrate(dialect_name, database_url):
    try:
        dialect = Dialect(dialect_name)
        adaptor = dialect.adaptor()
        # if dialect == Dialect.MYSQL:
        #     conn_info = json.loads(database_url)
        #     connection = adaptor.connect(**conn_info)
        # else:
        connection = adaptor.connect(database_url)
        assert not migration.Migration.database_migrations(connection)
        m = migration.Migration.key_load(EXISTING_MIGRATION_KEYS[0])
        migration.Migration.migrate(connection, dialect, m)

    finally:
        # clean up tables
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
