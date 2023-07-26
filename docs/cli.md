# CLI Usage

This page provides documentation for the sqly command line command.

The migration tools are both simple to use and very flexible. An example usage session:

```sh
# First, create a new migration file
$ sqly migration APP_NAME -n MIGRATION_NAME
Created migration: APP_NAME:20230725235258975_MIGRATION_NAME
    depends:
      - sqly:20211105034808482_init

# Now, edit the file to provide SQL for "up" and "dn"

# Then, you can list the migrations for APP_NAME
$ sqly migrations APP_NAME
APP_NAME:20230725235258975_MIGRATION_NAME

# Copy and paste that migration key to migrate the database to that revision.
$ sqly migrate APP_NAME:20230725235258975_MIGRATION_NAME -d $DATABASE_DIALECT -u $DATABASE_URL
...
sqly:20211105034808482_init up ... OK
APP_NAME:20230725235258975_MIGRATION_NAME up ... OK
```

::: mkdocs-click
    :module: sqly.__main__
    :command: sqly
    :style: table
