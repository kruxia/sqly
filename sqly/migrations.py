import asyncio
import json
import logging
import os
import re
from datetime import datetime
from importlib import import_module
from pathlib import Path

import click
import yaml

from sqly import queries
from sqly.dialects import Dialects

DB_REL_PATH = 'db'
log = logging.getLogger(__name__)
SQL_DIALECT = Dialects.ASYNCPG


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
    return create_migration(mod_name, label='init')


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
    for name in migrations_data:
        for key in ['requires', 'then_load']:
            if migrations_data[name].get(key) and isinstance(
                migrations_data[name][key], str
            ):
                migrations_data[name][key] = [migrations_data[name][key]]

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
    requires = last_migrations(migrations_data)
    requires += additional_requires or []
    migrations_data[migration_name] = {'requires': requires}
    if then_load:
        migrations_data[migration_name]['then_load'] = then_load
    with open(migrations_data_path / f"{migration_name}.up.sql", 'wb') as f:
        f.write(f''.encode('utf-8'))
    with open(migrations_data_path / f"{migration_name}.dn.sql", 'wb') as f:
        f.write(f''.encode('utf-8'))
    dump_migrations_data(mod_name, migrations_data)
    return migration_name


def sequence_migrations(data):
    """for a given set up migrations_data, sequence the migrations in apply order"""

    def get_migration_position(name):
        requires = data.get(name, {}).get('requires', [])
        if isinstance(requires, str):
            requires = [requires]
        if name not in data or not requires:
            return 0, name
        else:
            pos = (
                max([get_migration_position(req_name)[0] for req_name in requires]) + 1
            )
        return pos, name

    seq = sorted([get_migration_position(name) for name in data.keys()])
    return seq


def requirements(data, name):
    """generate a sequence of requirements for this migration in apply order"""
    requires = data.get(name, {}).get('requires') or []
    if isinstance(requires, str):
        requires = [requires]
    yielded = []
    for req_name in requires:
        for req in requirements(data, req_name):
            if req not in yielded:
                yield req
                yielded.append(req)
        if req_name not in yielded:
            yield req_name
            yielded.append(req_name)


def migrations_descendants(data):
    descendants = {}
    for name in data:
        descendants[name] = []
    for pos, name in sequence_migrations(data):
        requires = data[name].get('requires') or []
        if isinstance(requires, str):
            requires = [requires]
        for req_name in requires:
            descendants.setdefault(req_name, [])
            descendants[req_name].append(name)
    return descendants


def last_migrations(data):
    """return a list of migration names that have no descendants"""
    descendants = migrations_descendants(data)
    return sorted([name for name in descendants.keys() if not descendants[name]])


async def apply_migrations(db_settings, mod_name, names=None, down=True, conn=None):
    filepath = get_mod_filepath(mod_name) / DB_REL_PATH / 'migrations'
    data = load_migrations_data(mod_name)

    if not conn:
        asyncpg = import_module('asyncpg')
        conn = await asyncpg.connect(dsn=db_settings['dsn'])
        await conn.set_type_codec(
            'json', encoder=json.dumps, decoder=json.loads, schema='pg_catalog'
        )

    try:
        applied_migrations = [
            migration['name']
            for migration in await conn.fetch(
                "select * from __migrations order by run_at"
            )
        ]
    except:
        applied_migrations = []

    log.debug('applied_migrations = %r' % applied_migrations)

    if not names:
        names = last_migrations(data)
    if isinstance(names, str):
        names = [names]

    log.debug('names = %r' % names)

    names_sequence = [seq[1] for seq in sequence_migrations(data)]
    log.debug('names_sequence = %r' % names_sequence)

    # revert any descendants of the named migrations
    if down:
        all_descendants = migrations_descendants(data)
        descendants = set()
        for name in names:
            descendants |= set(all_descendants[name])
        for name in reversed(names_sequence):
            if name in descendants:
                await apply_migration_dn(
                    db_settings, conn, data, filepath, mod_name, name
                )
                if name in applied_migrations:
                    applied_migrations.pop(applied_migrations.index(name))

    # apply any ancestors of the named migrations, and the migrations themselves
    to_apply = []
    for name in names:
        requires = list(requirements(data, name))
        for req in requires:
            if req not in to_apply and req not in applied_migrations:
                to_apply.append(req)
    for name in names:
        if name not in to_apply and name not in applied_migrations:
            to_apply.append(name)

    for name in to_apply:
        await apply_migration_up(db_settings, conn, data, filepath, mod_name, name)


async def apply_migration_up(db_settings, conn, data, filepath, mod_name, name):
    if ':' in name:
        other_mod_name, migration_name = name.split(':')
        await apply_migrations(
            db_settings, other_mod_name, migration_name, conn=conn, down=False
        )
    else:
        log.info(f"{mod_name}:{name}:")
        migration_path = filepath / (name + '.up.sql')
        requires = data[name].get('requires')
        if requires and isinstance(requires, str):
            requires = [requires]
        with open(migration_path, 'rb') as f:
            sql = f.read().decode('utf-8').strip()
        async with conn.transaction():
            log.debug('\n   %s' % sql.replace('\n', '\n  '))
            if sql:
                result = await conn.execute(sql)
                log.debug('  %s' % result)
            result = await conn.fetchrow(
                "insert into __migrations (mod, name, requires) values ($1, $2, $3) returning *",
                mod_name,
                name,
                requires,
            )
            log.info('    %s' % yaml.dump(dict(**result)).replace('\n', '\n    '))
            then_load = data[name].get('then_load') or []
            if isinstance(then_load, str):
                then_load = [then_load]
            for to_load in then_load:
                log.info('  %s' % to_load)
                to_load_filepath = os.path.abspath(
                    os.path.join(migration_path.parent, to_load)
                )
                ext = os.path.splitext(to_load_filepath)[-1].lower()
                if ext == '.sql':
                    await load_sql_file(conn, to_load_filepath)
                elif ext in ['.json', '.yaml', '.yml']:
                    await load_data_file(conn, to_load_filepath)


async def load_sql_file(conn, filepath):
    with open(filepath, 'rb') as f:
        sql = f.read().decode('utf-8')
    result = await conn.execute(sql)
    log.debug('  %s' % result)


async def load_data_file(conn, filepath):
    ext = os.path.splitext(filepath)[-1].lower()
    with open(filepath, 'rb') as f:
        if ext == '.json':
            data = json.load(f)
        elif ext in ['.yaml', '.yml']:
            data = yaml.safe_load(f.read().decode('utf-8'))
        else:
            raise ValueError(f'Unsupported file extension: {ext}')

    table = data['table']
    records = data['records']
    primary_key = data.get('primary_key') or data.get('pk') or data.get('key') or []
    if isinstance(primary_key, str):
        primary_key = [primary_key]
    log.info(f"{table} {primary_key} {len(records)} records")

    query = queries.upsert(dialect=SQL_DIALECT)
    for record in records:
        sql, values = query.render(record, keys=primary_key, table=table)
        log.debug(f"{sql!r} {values!r}")
        result = await conn.execute(sql, *values)
        log.debug(result)


async def apply_migration_dn(db_settings, conn, data, filepath, mod_name, name):
    migration_path = filepath / (name + '.dn.sql')
    with open(migration_path, 'rb') as f:
        sql = f.read().decode('utf-8').strip()
    async with conn.transaction():
        if sql:
            result = await conn.execute(sql)
            log.info('   ', result.replace('\n', '\n    '))
        result = await conn.fetchrow(
            "delete from __migrations where mod=$1 and name=$2", mod_name, name
        )
        log.info('   ', result.replace('\n', '\n    '))


def get_database_settings(mod_name, settings_mod_name=None):
    if not settings_mod_name:
        settings_mod_name = f"{mod_name}.settings"
    try:
        database_settings = import_module(settings_mod_name).DATABASE
    except:
        database_settings = {'dsn': f'postgres://localhost/{mod_name.split(".")[0]}'}
    return database_settings


@click.group()
def main():
    pass


@main.command()
@click.argument('mod_name')
@click.option('-s', '--settings_mod_name', required=False)
@click.option('--log', default=logging.INFO)
def init(mod_name, settings_mod_name, log):
    logging.basicConfig(level=log)
    migration_name = init_app(mod_name)
    print(f"sqly migrations: initialized app: {mod_name}")
    print(f"sqly migrations: created migration: {mod_name}:{migration_name}")
    database_settings = get_database_settings(mod_name, settings_mod_name)
    asyncio.run(apply_migrations(mod_name, database_settings, migration_name))


@main.command()
@click.argument('mod_name')
@click.option('-l', '--label', default=None)
@click.option('--log', default=logging.INFO)
def create(mod_name, label, log):
    logging.basicConfig(level=log)
    migration_name = create_migration(mod_name, label=label)
    print(f"sqly migrations: created migration: {mod_name}:{migration_name}")


@main.command()
@click.argument('mod_name')
@click.option('-s', '--settings_mod_name', required=False)
@click.option('--log', default=logging.INFO)
@click.argument('migration_name', required=False)
def migrate(mod_name, settings_mod_name, log, migration_name=None):
    logging.basicConfig(level=log)
    database_settings = get_database_settings(mod_name, settings_mod_name)
    asyncio.run(apply_migrations(database_settings, mod_name, migration_name))


if __name__ == '__main__':
    main()
