# sqly

SQL is a fantastic language — one of the most successful programming languages in the world. We should use it, not try to replace it with a bespoke DSL. 

Yet there are a couple of things that are nice to have help with in constructing SQL queries:

* **dialect-aware safe value substitution**: Every database interface has its own syntax for substituting values safely (not to allow SQL injection) — for example, `$1` or `?` or `:varname`. They also have different requirements for the format of the sql + values argument lists. I want to able to write my queries with the same value substituion syntax, regardless of which database interface I am using, and know that the SQL will be output correctly for my interface, and that the values will be passed to the database engine safely. 
* **dynamic attributes**: In many applications, I don't know in advance which attributes I am going to select, insert, update, or filter by. I want to SELECT a given list of attributes, or filter WHERE a given key/value mapping, or UPDATE or INSERT particular attributes, without having to rewrite the SQL query.
* **block composition**: Some SQL queries are very complex. I want to able to compose blocks of SQL into larger queries, so that I can manage this complexity effectively. (Most database query DSLs are unable to deal with complex queries, or they invent a hard-to-learn language for writing those queries. Learning SQL is a better use of our time, but it would be very helpful having some assistance managing/manipulating the different blocks in a query.)

sqly:

* One class, `SQL`, with one field, `query`, and one method, `.render`, which takes one optional argument, `dialect`. 
* Dynamic value replacement, rendered in one of the supported dialects: postgres (`$1`), sqlalchemy (`:varname`), embedded (`:varname`), mysql (`%(varname)s`), sqlite (`?`). Default style is embedded / `:varname`.
* Dynamic attribute/value lists in `SELECT`,  `WHERE`, `INSERT`, and `UPDATE` syntax
* Block composition

