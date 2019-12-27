import json
import logging
import os
import re
import sys
from datetime import datetime
from importlib import import_module
from pathlib import Path

import yaml
from sqly import SQL, lib
from sqly.dialects import Dialects

DB_REL_PATH = 'db'
MIGRATIONS_YAML = 'migrations/__migrations.yaml'
SQL_DIALECT = Dialects.ASYNCPG
log = logging.getLogger('sqly.schema')


def init_app(mod_name, requires=None):
    """
    mod_name: an importable module name (dot-separated)

    * create the db folder and subfolders
    * create the first migration, which requires sqly:00000000000000_init
    """
    log.debug(f'init_app: {mod_name} {requires}')
    mod_path = get_mod_filepath(mod_name)
    for pathname in ['migrations', 'data', 'lib']:
        path = mod_path / DB_REL_PATH / pathname
        if not path.exists():
            os.makedirs(path)
    additional_requires = list(requires or []) + ['sqly:00000000000000_init']
    return create_migration(
        mod_name, label='init', additional_requires=additional_requires
    )


def get_mod_filepath(mod_name):
    mod = import_module(mod_name)
    mod_filepath = Path(mod.__file__).parent
    return mod_filepath


def load_migrations_data(mod_name):
    mod_filepath = get_mod_filepath(mod_name)
    migrations_data_filepath = mod_filepath / DB_REL_PATH / MIGRATIONS_YAML
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
    migrations_data_filepath = mod_filepath / DB_REL_PATH / MIGRATIONS_YAML
    if not migrations_data_filepath.parent.exists():
        os.makedirs(migrations_data_filepath.parent)
    with open(migrations_data_filepath, 'w') as f:
        f.write(yaml.dump(migrations_data))
    return migrations_data_filepath


def make_migration_name(label=None):
    if label is None:
        label = 'auto'
    else:
        label = re.sub(r'\W', '_', label.strip()).strip('_')
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    name = f"{timestamp}_{label}"
    return name


def create_migration(mod_name, label=None, additional_requires=None, then_load=None):
    """create a new migration based on previous migrations"""
    migrations_data = load_migrations_data(mod_name)
    migration_name = make_migration_name(label=label)
    assert migration_name not in migrations_data
    mod_filepath = get_mod_filepath(mod_name)
    migrations_data_path = mod_filepath / DB_REL_PATH / 'migrations'
    requires = tail_migrations(migrations_data)
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


def migration_requirements(data, name):
    """generate a sequence of requirements for this migration in apply order"""
    requires = data.get(name, {}).get('requires') or []
    if isinstance(requires, str):
        requires = [requires]
    yielded = []
    for req_name in requires:
        for req in migration_requirements(data, req_name):
            if req not in yielded:
                yield req
                yielded.append(req)
        if req_name not in yielded:
            yield req_name
            yielded.append(req_name)


def migrations_descendants(data):
    """return a list of descendants for each migration in data."""
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


def tail_migrations(data):
    """return a list of migration names that have no descendants"""
    descendants = migrations_descendants(data)
    return sorted([name for name in descendants.keys() if not descendants[name]])


def get_applied_migrations(conn):
    try:
        applied_migrations = lib.run(
            conn.fetch("select * from __migrations order by run_at")
        )
    except Exception:
        print(sys.exc_info()[1])
        applied_migrations = []

    return applied_migrations


def apply_migrations(conn, mod_name, names=None, down=True):
    filepath = get_mod_filepath(mod_name) / DB_REL_PATH / 'migrations'
    data = load_migrations_data(mod_name)

    applied_migrations = get_applied_migrations(conn)
    applied_migrations_names = [migration['name'] for migration in applied_migrations]

    log.debug('applied migrations = %r' % applied_migrations_names)

    # ensure that the given migrations are a list; default to the tail_migrations
    if not names:
        names = tail_migrations(data)
    elif isinstance(names, str):
        names = [names]

    log.debug('migration names = %r' % names)

    if down:
        # revert any descendants of the named migrations
        applied_migrations = revert_migrations_descendants(
            conn, data, mod_name, names, filepath, applied_migrations
        )

    # apply any ancestors of the named migrations, and the migrations themselves
    to_apply = []
    for name in names:
        requires = list(migration_requirements(data, name))
        for req in requires:
            if req not in to_apply and req not in applied_migrations_names:
                to_apply.append(req)
    for name in names:
        if name not in to_apply and name not in applied_migrations_names:
            to_apply.append(name)

    for name in to_apply:
        apply_up_migration(conn, data, filepath, mod_name, name)


def revert_migrations(conn, mod_name, names):
    filepath = get_mod_filepath(mod_name) / DB_REL_PATH / 'migrations'
    data = load_migrations_data(mod_name)

    applied_migrations = get_applied_migrations(conn)
    applied_migrations_names = [migration['name'] for migration in applied_migrations]
    applied_migrations = revert_migrations_descendants(
        conn, data, mod_name, names, filepath, applied_migrations
    )
    for name in names:
        if name not in applied_migrations_names:
            apply_dn_migration(conn, data, mod_name, name, filepath)


def revert_migrations_descendants(
    conn, data, mod_name, names, filepath, applied_migrations
):
    names_sequence = [seq[1] for seq in sequence_migrations(data)]
    all_descendants = migrations_descendants(data)
    descendants = set()
    for name in names:
        descendants |= set(all_descendants[name])
    log.debug('descendants = %r' % descendants)
    for name in reversed(names_sequence):
        if name in descendants and name in [
            migration['name'] for migration in applied_migrations
        ]:
            lib.run(apply_dn_migration(conn, data, mod_name, name, filepath))
            migration = [
                migration
                for migration in applied_migrations
                if migration['name'] == name
            ][0]
            applied_migrations.pop(applied_migrations.index(migration))
    return applied_migrations


def apply_up_migration(conn, data, filepath, mod_name, name):
    if ':' in name:
        # ensure that the migration in the other module is applied, without migrating down
        other_mod_name, migration_name = name.split(':')
        apply_migrations(conn, other_mod_name, migration_name, down=False)
    else:
        log.info(f'apply up migration: {mod_name}:{name} ({filepath})')
        migration_path = filepath / (name + '.up.sql')
        requires = data.get(name, {}).get('requires')
        if requires and isinstance(requires, str):
            requires = [requires]
        with open(migration_path, 'rb') as f:
            sql = f.read().decode('utf-8').strip()

        lib.run(conn.execute('BEGIN'))

        log.debug('\n   %s' % sql.replace('\n', '\n  '))
        if sql:
            result = lib.run(conn.execute(sql))
            log.debug('  %s' % result)
        result = lib.run(
            conn.fetchrow(
                "insert into __migrations (mod, name, requires) values ($1, $2, $3) returning *",
                mod_name,
                name,
                requires,
            )
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
                load_sql_file(conn, to_load_filepath)
            elif ext in ['.json', '.yaml', '.yml']:
                load_data_file(conn, to_load_filepath)

        lib.run(conn.execute('COMMIT'))


def apply_dn_migration(conn, data, mod_name, name, filepath):
    log.info(f'apply dn migration: {mod_name}:{name} ({filepath})')
    migration_path = filepath / (name + '.dn.sql')
    with open(migration_path, 'rb') as f:
        sql = f.read().decode('utf-8').strip()

    lib.run(conn.execute('BEGIN'))

    if sql:
        result = lib.run(conn.execute(sql))
        log.info('    %s' % result.replace('\n', '\n    '))
    lib.run(
        conn.fetchrow(
            "delete from __migrations where mod=$1 and name=$2", mod_name, name
        )
    )

    lib.run(conn.execute('COMMIT'))


def load_sql_file(conn, filepath):
    with open(filepath, 'rb') as f:
        sql = f.read().decode('utf-8')
    result = lib.run(conn.execute(sql))
    log.debug('  %s' % result)


def load_data_file(conn, filepath):
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

    query = SQL(SQL_DIALECT).upsert()
    for record in records:
        sql, values = query.render(record, keys=primary_key, table=table)
        log.debug(f"{sql!r} {values!r}")
        result = lib.run(conn.execute(sql, *values))
        log.debug(result)
