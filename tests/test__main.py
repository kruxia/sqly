import os
from glob import glob
from pathlib import Path

import pytest
import yaml

import sqly
import testapp
from sqly import __main__
from tests import fixtures

SQLY_MIGRATIONS_PATH = Path(sqly.__file__).absolute().parent / "migrations"
TESTAPP_MIGRATIONS_PATH = Path(testapp.__file__).absolute().parent / "migrations"


def test_main_migration(cli_runner):
    """
    Creating a migration with a given name results in a new migration that depends on
    the "leaf" migrations in the app.
    """
    try:
        # m01 will depend on the latest sqly migration leaf node(s)
        cli_runner.invoke(__main__.migration, ["testapp", "-n", "m01"])
        m01_path = Path(next(iter(glob(str(TESTAPP_MIGRATIONS_PATH / "*_m01.yaml")))))
        m01_key = f"testapp:{m01_path.stem}"
        with open(m01_path) as f:
            m01_data = yaml.safe_load(f)
        print(f"{m01_data=}")
        assert all([("sqly" in key) for key in m01_data["depends"]])

        # m02 will depend on m01
        cli_runner.invoke(__main__.migration, ["testapp", "-n", "m02"])
        m02_path = Path(next(iter(glob(str(TESTAPP_MIGRATIONS_PATH / "*_m02.yaml")))))
        m02_key = f"testapp:{m02_path.stem}"
        with open(m02_path) as f:
            m02_data = yaml.safe_load(f)
        print(f"{m02_data=}")
        assert m02_data["depends"] == [m01_key]

        # make m02 depends the same as m01 depends (so both are leaf nodes)
        m02_data["depends"] = m01_data["depends"]
        with open(m02_path, "w") as f:
            f.write(yaml.dump(m02_data))

        # m03 will depend on m01 and m02
        cli_runner.invoke(__main__.migration, ["testapp", "-n", "m03"])
        m03_path = Path(next(iter(glob(str(TESTAPP_MIGRATIONS_PATH / "*_m03.yaml")))))
        with open(m03_path) as f:
            m03_data = yaml.safe_load(f)
        print(f"{m03_data=}")
        assert sorted(m03_data["depends"]) == [m01_key, m02_key]

    finally:
        # clean up testapp migrations
        testapp_migrations = glob(str(TESTAPP_MIGRATIONS_PATH / "*.yaml"))
        for filename in testapp_migrations:
            os.remove(filename)


def test_main_migrations(cli_runner):
    """
    - Listing migrations for an app includes the migrations in that app.
    - Listing migrations including dependencies also includes those with DAG notation.
    """
    try:
        # create a testapp migration
        cli_runner.invoke(__main__.migration, ["testapp", "-n", "m01"])

        # get sqly and testapp migrations
        sqly_migration_paths = glob(str(SQLY_MIGRATIONS_PATH / "*.yaml"))
        testapp_migration_paths = glob(str(TESTAPP_MIGRATIONS_PATH / "*.yaml"))

        sqly_migration_keys = [
            f"sqly:{Path(path).stem}" for path in sqly_migration_paths
        ]
        testapp_migration_keys = [
            f"testapp:{Path(path).stem}" for path in testapp_migration_paths
        ]

        # listing testapp migrations without dependencies does not include sqly
        result = cli_runner.invoke(__main__.migrations, ["testapp"])
        print(f"{result.output=}")
        assert all([(path in result.output) for path in testapp_migration_keys])
        assert not any([(path in result.output) for path in sqly_migration_keys])

        # listing testapp migrations with dependencies includes sqly as migrations and
        # dependencies
        result = cli_runner.invoke(
            __main__.migrations, ["testapp", "--include-depends"]
        )
        print(f"{result.output=}")
        assert all([(key in result.output) for key in testapp_migration_keys])
        assert all([(key in result.output) for key in sqly_migration_keys])
        # dependencies are listed as '=> {key}'
        assert all([(f"=> {key}" in result.output) for key in sqly_migration_keys])

    finally:
        # clean up testapp migrations
        testapp_migrations = glob(str(TESTAPP_MIGRATIONS_PATH / "*.yaml"))
        for filename in testapp_migrations:
            os.remove(filename)


@pytest.mark.parametrize("dialect_name,database_url", fixtures.test_databases)
def test_main_migrate(cli_runner, dialect_name, database_url):
    try:
        # create a testapp migration that creates a table
        cli_runner.invoke(__main__.migration, ["testapp", "-n", "widgets"])
        m_path = Path(next(iter(glob(str(TESTAPP_MIGRATIONS_PATH / "*_widgets.yaml")))))
        m_key = f"testapp:{m_path.stem}"
        with open(m_path) as f:
            m_data = yaml.safe_load(f)
        m_data["up"] = "CREATE TABLE widgets (id int, sku varchar)"
        m_data["dn"] = "DROP TABLE widgets"
        with open(m_path, "w") as f:
            f.write(yaml.dump(m_data))

        # migrate up
        result = cli_runner.invoke(
            __main__.migrate, [m_key, "-u", database_url, "-d", dialect_name]
        )
        assert result.exit_code == 0
        assert m_key in result.output

        # migrate down
        sqly_init_path = Path(
            next(iter(sorted(glob(str(SQLY_MIGRATIONS_PATH / "*.yaml")))))
        )
        sqly_init_key = f"sqly:{sqly_init_path.stem}"
        print(sqly_init_key)
        result = cli_runner.invoke(
            __main__.migrate, [sqly_init_key, "-u", database_url, "-d", dialect_name]
        )
        assert result.exit_code == 0

    finally:
        # clean up testapp migrations
        testapp_migrations = glob(str(TESTAPP_MIGRATIONS_PATH / "*.yaml"))
        for filename in testapp_migrations:
            os.remove(filename)

        # clean up database file if any
        db_file = database_url.split("file://")[-1] if "file://" in database_url else ""
        if os.path.exists(db_file):
            os.remove(db_file)


@pytest.mark.parametrize("dialect_name,database_url", fixtures.test_databases)
def test_main_migrate_dryrun(cli_runner, dialect_name, database_url):
    """
    The same migration run twice as a dryrun will have the same output and exit 0,
    because it wasn't applied.
    """
    try:
        m_path = Path(next(iter(glob(str(SQLY_MIGRATIONS_PATH / "*.yaml")))))
        m_key = f"sqly:{m_path.stem}"

        # migrate up
        result1 = cli_runner.invoke(
            # -r = --dryrun
            __main__.migrate,
            [m_key, "-r", "-u", database_url, "-d", dialect_name],
        )
        assert result1.exit_code == 0

        result2 = cli_runner.invoke(
            __main__.migrate,
            [m_key, "--dryrun", "-u", database_url, "-d", dialect_name],
        )
        assert result2.exit_code == 0

        assert result1.output == result2.output

    finally:
        # clean up database file if any
        db_file = database_url.split("file://")[-1] if "file://" in database_url else ""
        if os.path.exists(db_file):
            os.remove(db_file)


@pytest.mark.parametrize("dialect_name,database_url", fixtures.test_databases)
def test_main_migrate_invalid(cli_runner, dialect_name, database_url):
    try:
        # create a testapp migration that creates a table
        cli_runner.invoke(__main__.migration, ["testapp", "-n", "widgets"])
        m_path = Path(next(iter(glob(str(TESTAPP_MIGRATIONS_PATH / "*_widgets.yaml")))))
        m_key = f"testapp:{m_path.stem}"
        with open(m_path) as f:
            m_data = yaml.safe_load(f)
        m_data["up"] = "CREATE TABLE widgets (id int, sku varchar)"
        m_data["dn"] = "DROP TABLE widgets"
        with open(m_path, "w") as f:
            f.write(yaml.dump(m_data))

        # migrate up without database url fails
        result = cli_runner.invoke(__main__.migrate, [m_key, "-d", dialect_name])
        assert result.exit_code == 1

        # migrate up without database dialect fails
        result = cli_runner.invoke(__main__.migrate, [m_key, "-u", database_url])
        assert result.exit_code == 1

    finally:
        # clean up testapp migrations
        testapp_migrations = glob(str(TESTAPP_MIGRATIONS_PATH / "*.yaml"))
        for filename in testapp_migrations:
            os.remove(filename)

        # clean up database file if any
        db_file = database_url.split("file://")[-1] if "file://" in database_url else ""
        if os.path.exists(db_file):
            os.remove(db_file)
