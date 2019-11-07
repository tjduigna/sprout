# sprout


A wrapper around some python libraries for postgres.

 - asyncpg
 - tortoise

sprout attempts to perform db schema management for
python packages that match the specified structure:

`$pkg.orm.$schema.$table.py`

The initial aim is to provide an object-oriented interface
to be embedded in applications, as well as expose a
command line interface. sprout would like to support
the following functionality:

  - Simple database initialization, inspection and interaction
  - A container for multiple tortoise ORM table models
  - Convenient API for asynchronous interaction with the database

Ultimately, the goal is to include some primitive
support for ORM table definition based schema migrations.


A setup for a package that works with sprout looks like
(likely subject to change before a final design):

```
$pkg/__init__.py -> from $pkg import orm
    orm/__init__.py -> from .$schema import *
    orm/$schema/__init__.py -> from .$table import $Table
    orm/$schema/$table.py -> table definitions
```
