# Basic Usage

Once sqly is installed in your environment, you can:

* use the CLI to manage database migrations. See [CLI Usage](cli.md).
* use the `sqly` module to manage your interactions with a SQL database.

Here is an example session working with sqly. Let's assume that our python project is
called `my_project` and we're in the package directory in the shell. We're going to use
a SQLite database in a file called `my_project.db`. 

First, let's create a table called `products` using sqly migrations. 

```sh
$ sqly migration my_app -n products
Created migration: my_app:20230726163514429_products
    depends:
      - sqly:20211105034808482_init
```

Now we can edit the created YAML file:
```yaml
# my_app/migrations/20230726163514429_products.yaml
app: my_app
ts: 20230726163514429
name: products
depends:
- sqly:20211105034808482_init
doc: null
up: |-
  CREATE TABLE products (
    id        integer PRIMARY KEY AUTOINCREMENT,
    name      varchar NOT NULL,
    sku       varchar NOT NULL UNIQUE,
    created   datetime DEFAULT current_timestamp
  )
dn: DROP TABLE products
```

Now we can apply the migration using the migration key,
`my_app:20230726163514429_products`. It's also useful to define the DATABASE_URL and
DATABASE_DIALECT in the environment so we don't have to pass them as commandline
parameters and so we can use them in Python. (This is where I usually recommend using
the excellent [direnv](https://direnv.net/) to manage project-level environment
variables. Just make sure to add the `.envrc` file to your `.gitignore`.)
```sh
$ export DATABASE_URL=my_app.db >>.envrc
$ export DATABASE_DIALECT=sqlite >>.envrc
$ direnv allow
...
$ sqly migrate my_app:20230726163514429_products
no such table: sqly_migrations
sqly:20211105034808482_init up ... OK
my_app:20230726163514429_products up ... OK
```
The first time migrations are run, there is not yet a `sqly_migrations` table, but
that's ok: the sqly init migration creates it. 

If we want to make changes to our products migration, we can roll it back, change the
YAML, and the re-migrate up. The way to roll back is to select the migration we want to
migrate to -- in this case, the sqly init migration:
```sh
$ sqly migrate sqly:20211105034808482_init 
my_app:20230726163514429_products dn ... OK
$ # ... make some changes ...
$ sqly migrate my_app:20230726163514429_products
my_app:20230726163514429_products up ... OK
```

Now that we have a database with a table, we can interact with it in our application
code.

```py
import os
from sqly import SQL, queries

DATABASE_DIALECT = os.environ['DATABASE_DIALECT']
DATABASE_URL = os.environ['DATABASE_URL']

# set up the database connection and SQL interface
sql = SQL(dialect=DATABASE_DIALECT)
adaptor = sql.dialect.adaptor()
conn = adaptor.connect(DATABASE_URL)

# insert some products
products = [
    {'name': 'cheese grater', 'sku': 'product-01'},
    {'name': 'cheese slicer', 'sku': 'product-02'},
    {'name': 'fondue pot', 'sku': 'product-03'},
]
for product in products:
    sql.execute(conn, queries.INSERT('products', product), product)

conn.commit()

# select some products
cheese_products = sql.select(
    conn, 
    queries.SELECT(
        'products', 
        filters=["name like :pattern"]
    ), 
    {'pattern': 'cheese%'},
)
for product in cheese_products:
    print(product)
# {'id': 1, 'name': 'cheese grater', 'sku': 'product-01', 'created': ...}
# {'id': 2, 'name': 'cheese slicer', 'sku': 'product-02', 'created': ...}

# delete a product that no self-respecting cheeseshop would carry
sql.execute(
    conn, 
    queries.DELETE('products', filters=["name like :pattern"]), {'pattern': 'fondue%'},
)
# on second thought, let's just rename it so it's more cheesy
conn.rollback()
sql.execute(
    conn,
    queries.UPDATE(
        'products', 
        ['name'], 
        filters=["name like :pattern"], 
    ),
    {'name': 'cheese melter', 'pattern': 'fondue%'}
)
conn.commit()
```