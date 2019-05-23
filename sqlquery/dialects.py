from enum import Enum


class Dialects(Enum):
    POSTGRES = ['$i', 'postgresql', 'postgres', 'asyncpg', 'pg']
    MYSQL = ['%(var)s', 'mysql', 'psycopg']
    EMBEDDED = [':var', 'sqlalchemy', 'embedded']
    SQLITE = ['?', 'sqlite']
