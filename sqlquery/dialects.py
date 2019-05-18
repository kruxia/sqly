from enum import Enum


class Dialects(Enum):
    POSTGRES = ['$i', 'postgresql', 'postgres', 'asyncpg']
    MYSQL = ['%(var)s', 'mysql', 'psycopg']
    EMBEDDED = [':var', 'sqlalchemy', 'embedded']
    SQLITE = ['?', 'sqlite']
