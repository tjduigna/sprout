# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0
"""Dump the state of a DB schema to files.
This is not implemented to scale."""

import sys
import asyncio
import argparse
import importlib

import asyncpg
import pandas as pd
from tortoise import fields

import sprout

_BLACKLIST = ['id']

# command line utility
parser = argparse.ArgumentParser(description="Dump application DB schema")
parser.add_argument('--appname', required=True, help="name of application")
parser.add_argument('--schema', required=True, help="schema where table resides")
parser.add_argument('--table', required=True, help="name of the table")


async def dump_table(appname, schema, table):
    """Dump the data in a table to csv,
    ignoring the 'id' field.

    Args:
        appname (str): application name
        schema (str): schema name
    """
    #base = sprout.cfg.db_str(appname)
    #con = await asyncpg.connect(base)

    # TODO : lifted from update_db.model_state
    await sprout.init_db(appname, [schema])
    modkey = f"{appname}.orm.{schema}.{table}"
    importlib.import_module(modkey)
    mod = sys.modules[modkey]
    obj = vars(mod)[table.title()]
    dat = await obj.filter()
    keys = [key for key in vars(dat[0]).keys()
            if not key.startswith('_') and key
            not in _BLACKLIST]
    pd.DataFrame.from_dict(
        ({k: getattr(row, k) for k in keys} for row in dat)
    ).to_csv(f'{modkey}.csv', index=False)
#    names = [key for key in vars(obj).keys()
#             if not key.startswith('_')]
#    fields = [getattr(obj, fld) for fld in names]
#    print(names)
#    print(fields)


async def dump_db(appname, schema):
    """Dump the data in an application database
    to csv files for intra-database migration.

    Args:
        appname (str): application name
        schema (str): schema name
    """
    base = sprout.cfg.db_str(appname)
    con = await asyncpg.connect(base)



if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(
        dump_table(args.appname,
                   args.schema,
                   args.table)
    )
