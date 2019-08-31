import os
import re
from datetime import datetime
from importlib import import_module
from pathlib import Path

import click

import yaml

DB_REL_PATH = 'db'


def init_app(mod_name):
    """
    mod_name: an importable module name (dot-separated)

    * create the db folder and subfolders
    * create the first migration, which requires sqly:00000000000000_init
    """
    mod_path = get_mod_filepath(mod_name)
    for pathname in ['migrations', 'data', 'lib']:
        path = mod_path / pathname
        if not path.exists():
            os.makedirs(path)
    return create_migration(
        mod_name, label='init', additional_requires=['sqly:00000000000000_init']
    )


def get_mod_filepath(mod_name):
    mod = import_module(mod_name)
    mod_filepath = Path(mod.__file__).parent
    return mod_filepath


def load_migrations_data(mod_name):
    mod_filepath = get_mod_filepath(mod_name)
    migrations_data_filepath = (
        mod_filepath / DB_REL_PATH / 'migrations' / '__migrations.yaml'
    )
    if migrations_data_filepath.exists():
        migrations_data = yaml.safe_load(open(migrations_data_filepath))
    else:
        migrations_data = {}
    return migrations_data


def dump_migrations_data(mod_name, migrations_data):
    mod_filepath = get_mod_filepath(mod_name)
    migrations_data_filepath = (
        mod_filepath / DB_REL_PATH / 'migrations' / '__migrations.yaml'
    )
    if not migrations_data_filepath.parent.exists():
        os.makedirs(migrations_data_filepath.parent)
    with open(migrations_data_filepath, 'w') as f:
        f.write(yaml.dump(migrations_data))
    return migrations_data_filepath


def make_migration_name(label=None):
    name = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_"
    if label:
        name += re.sub(r'\W', '_', label.strip()).strip('_')
    else:
        name += 'auto'
    return name


def create_migration(mod_name, label=None, additional_requires=None, then_load=None):
    """create a new migration based on previous migrations"""
    migrations_data = load_migrations_data(mod_name)
    migration_name = make_migration_name(label=label)
    assert migration_name not in migrations_data
    mod_filepath = get_mod_filepath(mod_name)
    migrations_data_path = mod_filepath / DB_REL_PATH / 'migrations'
    requires = []
    requires += additional_requires or []
    migrations_data[migration_name] = {'requires': requires}
    if then_load:
        migrations_data[migration_name]['then_load'] = then_load
    with open(migrations_data_path / f"{migration_name}.up.sql", 'wb') as f:
        f.write(f'-- \n'.encode('utf-8'))
    with open(migrations_data_path / f"{migration_name}.dn.sql", 'wb') as f:
        f.write(f'-- \n'.encode('utf-8'))
    dump_migrations_data(mod_name, migrations_data)
    return migration_name


@click.group()
def main():
    pass


@main.command()
@click.argument('mod_name')
def init(mod_name):
    migration_name = init_app(mod_name)
    print(f"sqly migrations: initialized app: {mod_name}")
    print(f"sqly migrations: created migration: {mod_name}:{migration_name}")


@main.command()
@click.argument('mod_name')
@click.option('-l', '--label', default=None)
def create(mod_name, label):
    migration_name = create_migration(mod_name, label=label)
    print(f"sqly migrations: created migration: {mod_name}:{migration_name}")


if __name__ == '__main__':
    main()
