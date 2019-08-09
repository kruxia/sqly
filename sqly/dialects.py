from enum import Enum

class Dialects(Enum):
    EMBEDDED = ':varname'
    MYSQL = '%(varname)s'
    SQLITE = '?' 
    POSTGRES = '$i'
    ASYNCPG = POSTGRES
    PSYCOPG2 = MYSQL
    SQLALCHEMY = EMBEDDED
