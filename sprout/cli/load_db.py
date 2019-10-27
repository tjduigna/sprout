# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0
"""Load the dumped data from dump_db.py
into a different database."""

import sys
import asyncio
import argparse
import importlib

import asyncpg
import pandas as pd
from tortoise import fields

import sprout

async def load_table(appname, schema, table):
    """Load the data from a csv to the table.

    Args:
        appname (str): application name
        schema (str): schema name
        table (str): table name
    """
    await sprout.init_db(appname, [schema])
    pool = await sprout.db_pool(appname)
    csv = f"{appname}.orm.{schema}.{table}.csv"
    df = pd.read_csv(csv, keep_default_na=False)
    async with pool.acquire() as con:
        thing = await con.fetch("select * from information_schema.columns"
        " where table_schema='food' and table_name='ingredient';")
        print(thing)
    async with pool.acquire() as con:
        await con.copy_records_to_table(
            table, schema_name=schema,
            records=(tuple(x) for x in df.values),
            columns=list(df.columns),
            timeout=10
        )


# command line utility
parser = argparse.ArgumentParser(description="Dump application DB schema")
parser.add_argument('--appname', required=True, help="name of application")
parser.add_argument('--schema', required=True, help="schema where table resides")
parser.add_argument('--table', required=True, help="name of the table")


if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(
        load_table(args.appname,
                   args.schema,
                   args.table)
    )
