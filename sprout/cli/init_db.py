# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0
"""Establish databases, schemas and tables for
an application that conforms to the expectation
of the sprout library."""

import asyncio
import argparse

import asyncpg
from tortoise import fields
from tortoise import Tortoise

import sprout


async def _create_database(root, name):
    """Create a new database with a root database
    connection.

    Args:
        root (str): a root db connection string
        name (str): name of the application db
    """
    con = await asyncpg.connect(root)
    try:
        await con.execute(f"create database {name};")
    except asyncpg.exceptions.DuplicateDatabaseError:
        sprout.cfg.log.info(f"database {name} exists")
    finally:
        await con.close()


async def _create_schema(base, name):
    """Create a new database schema with an application
    database connection.

    Args:
        base (str): a db schema connection string
        name (str): name of the application db
    """
    con = await asyncpg.connect(base)
    try:
        await con.execute(f"create schema {name};")
    except asyncpg.exceptions.DuplicateSchemaError:
        sprout.cfg.log.info(f"schema {name} exists")
    finally:
        await con.close()


async def init_db(appname, schemas, db_str):
    """Bring up a new application database and specified
    schemas

    Args:
        appname (str): application name
        schemas (list): list of schemas
        db_str (str): connection string
    """
    # root = db_str # or sprout.cfg.db_str()
    # await _create_database(root, appname)
    # base = sprout.cfg.db_str(appname)
    for schema in schemas:
        await _create_schema(db_str, schema)
        await Tortoise.init(
            db_url=db_str, #sprout.cfg.db_str(appname, schema),
            modules={'models': [f'{appname}.orm.{schema}']}
        )
        await Tortoise.generate_schemas()
        sprout.cfg.log.info(f"{schema} ready")


async def db_pool(appname, db_opts):
    """Create an application database connection pool.

    Args:
        appname (str): application name
        db_opts (dict): database params

    Returns:
        pool: asyncpg connection pool
    """
    opts = db_opts # or sprout.cfg.db_opts.copy()
    opts['database'] = appname
    opts['user'] = opts.pop('username')
    opts.pop('driver')
    pw = opts.pop('password')
    sprout.cfg.log.debug('asyncpg pool init')
    sprout.cfg.log.debug(opts)
    opts['password'] = pw
    pool = await asyncpg.create_pool(**opts)
    return pool


# command line utility
parser = argparse.ArgumentParser(description="Initialize application DB schema")
parser.add_argument('--appname', required=True, help="name of application")
parser.add_argument('--schemas', required=True, help="comma separated schemas")
parser.add_argument('--config', required=False, help="path to db config file")


if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(
        init_db(appname=args.appname,
                schemas=args.schemas.split(','),
                config=args.config)
    )
