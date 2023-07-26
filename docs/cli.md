# CLI Usage

This page provides documentation for the sqly command line command. 

* [`sqly migration`](#sqly-migration) = create a new migration. Example:
  ```sh
  $ sqly migration APP_NAME -n MIGRATION_NAME
  Created migration: APP_NAME:20230725235258975_MIGRATION_NAME
      depends:
        - sqly:20211105034808482_init
  ```

* [`sqly migrations`](#sqly-migrations) = list the migrations. Example:
  ```sh
  $ sqly migrations APP_NAME
  APP_NAME:20230725235258975_MIGRATION_NAME
  ```

* [`sqly migrate`](#sqly-migrate) = migrate the database to a migration. Example:
  ```sh
  $ sqly migrate APP_NAME:20230725235258975_MIGRATION_NAME
  ...
  sqly:20211105034808482_init up ... OK
  APP_NAME:20230725235258975_MIGRATION_NAME up ... OK
  ```

::: mkdocs-click
    :module: sqly.__main__
    :command: sqly
    :style: table
