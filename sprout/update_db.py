# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0
"""Run simple database migrations by inspecting
the current state of the database and comparing
it to the current ORM table definitions."""

import sys
import asyncio
import asyncpg
import inspect
import argparse
import importlib

import pandas as pd
from tortoise import fields

import sprout


_pg_type_map = {
    fields.IntField: 'integer',
    #fields.BigIntField: 'biginteger',
    #fields.SmallIntField: 'smallinteger',
    #fields.CharField: 'character',
    fields.TextField: 'text',
    fields.BooleanField: 'boolean',
    fields.DecimalField: 'numeric',
    #fields.DateTimeField: 'timestamp',
    #fields.DateField: 'date',
    #fields.TimeDeltaField: 'timedelta',
    fields.FloatField: 'double precision',
    #fields.JSONField: 'json',
    #fields.UUIDField: 'uuid',
    #fields.ForeignKeyField,
    #fields.ManyToManyField,
    #fields.BackwardFKRelation,
}


async def schema_info(appname, schema):
    base = sprout.cfg.db_str(appname)
    con = await asyncpg.connect(base)
    cols = [
        'table_catalog',
        'table_schema',
        'table_name',
        'data_type',
        'column_name',
    ]
    q = f"""select {', '.join(cols)} from information_schema.columns
where table_catalog='{appname}' and table_schema='{schema}';"""
    cols = cols[:-2] + ['constraint_name']
    k = f"""select {', '.join(cols)} from information_schema.table_constraints
where table_catalog='{appname}' and table_schema='{schema}'
and constraint_type='FOREIGN KEY';"""
    try:
        ret = await con.fetch(q)
        df = pd.DataFrame.from_records(ret, columns=ret[0].keys())
        print("Information Schema Summary")
        print(df)
        ret = await con.fetch(k)
        fk = pd.DataFrame.from_records(ret, columns=ret[0].keys())
        print("Foreign Key Summary")
        print(fk)
    except Exception as e:
        sprout.cfg.log.error(f"fetching current table state failed: {e}")
    finally:
        await con.close()


async def schema_state(appname, schema, table):
    """Get the current table definition that exists
    in the database.
    
    Args:
        appname (str): application (database) name
        schema (str): schema name
        table (str): table name

    Returns:
        df (pd.DataFrame): description of table schema
    """
    base = sprout.cfg.db_str(appname)
    con = await asyncpg.connect(base)
    req = ', '.join([
        'table_catalog',
        'table_schema',
        'table_name',
        'column_name',
        'data_type'
    ])
    q = f"""select {req} from information_schema.columns
where table_schema='{schema}' and table_name='{table}';"""
    try:
        ret = await con.fetch(q)
        df = pd.DataFrame.from_records(ret, columns=ret[0].keys())
        return df
    except Exception as e:
        sprout.cfg.log.error(f"fetching current table state failed: {e}")
    finally:
        await con.close()


def model_state(appname, schema, table):
    """Get thee current table definition as specified
    by the Tortoise ORM model definition.

    Args:
        appname (str): application name
        schema (str): schema name
        table (str): table name

    Returns:
        df (pd.DataFrame): description of table model

    Note:
        Assumes appname has a module structure organized
        like: f"{appname}.orm.{schema}.{table}"
    """
    modkey = f"{appname}.orm.{schema}.{table}"
    importlib.import_module(modkey)
    mod = sys.modules[modkey]
    obj = vars(mod)[table.title()]
    inst = obj()
    names = [key for key in vars(obj).keys()
             if not key.startswith('_')]
    fields = [getattr(obj, fld) for fld in names]
    flds, typs = [], []
    print("Inspecting the state of the model")
    for name, field in zip(names, fields):
        print(name, field, type(field))
        if not isinstance(field, property):
            flds.append(name)
            typs.append(field)
    print(typs)
    print(_pg_type_map)
    typs = [_pg_type_map[fld.__class__]
            for fld in typs]
    print("flds", flds)
    print("typs", typs)
    return pd.DataFrame.from_dict({
        'table_catalog': appname,
        'table_schema': schema,
        'table_name': table,
        'column_name': flds,
        'data_type': typs
    })


def compare_state(appname, schema, table, dbstate, modstate):
    """Compare the schema of tables and generate raw sql
    to upgrade the DB schema to match the ORM model and
    downgrade the DB schema to match the ORM model.
    """
    dbstate['id'] = range(len(dbstate.index))
    modstate['id'] = range(len(modstate.index))
    print("dbstate")
    print(dbstate)
    print("modstate")
    print(modstate)
    match = [
        'table_catalog',
        'table_schema',
        'table_name',
        'column_name',
        'data_type',
    ]
    kws = { 'on': match, 'suffixes': ('_mod', '_db')}
    up = pd.merge(modstate, dbstate, how='left', **kws)
    dn = pd.merge(modstate, dbstate, how='right', **kws)
    print("updf")
    print(up)
    print("dndf")
    print(dn)
    new = up[up['id_db'].isnull()]
    old = dn[dn['id_mod'].isnull()]
    base_sql = f"alter table {schema}.{table}"
    upg_sql = f"{base_sql} add column {{}} {{}} default null;"""
    upg_sql = '\n'.join([upg_sql.format(colname, data_type)
                         for colname, data_type in zip(
                         new['column_name'], new['data_type'])])
    dng_sql = f"{base_sql} drop column {{}};"""
    dng_sql = '\n'.join([dng_sql.format(colname)
                         for colname in old['column_name']])
    return upg_sql, dng_sql


async def set_state(appname, diff):
    base = sprout.cfg.db_str(appname)
    con = await asyncpg.connect(base)
    try:
        await con.execute(diff)
    except Exception as e:
        sprout.cfg.log.error(f"failed setting state: {e}")
    finally:
        await con.close()


# command line utility
parser = argparse.ArgumentParser(description="Update application DB schema")
parser.add_argument('--appname', required=True, help="name of application")
parser.add_argument('--schema', required=True, help="schema where table resides")
parser.add_argument('--table', required=False, help="name of table to inspect")
parser.add_argument('--action', required=False, help="upgrade or downgrade")


async def update_db(appname, schema, table, action):
    """Compare the state of a table in a database to
    its ORM model table definition. Optionally run
    statements to upgrade or downgrade the database
    corresponding to the ORM model.

    Args:
        appname (str): application name
        schema (str): schema name
        table (str): table name (required if action != 'info')
        action (str): 'upgrade', 'downgrade' or 'info'
    """
    loop = asyncio.get_event_loop()
    if args.action == 'info':
        await schema_info(args.appname, args.schema)
        sys.exit()

    dbstate = await schema_state(args.appname, args.schema, args.table)
    modstate = model_state(args.appname, args.schema, args.table)
    updiff, downdiff = compare_state(args.appname, args.schema, args.table,
                                     dbstate, modstate)
    if args.action == 'upgrade':
        await set_state(args.appname, updiff)
    elif args.action == 'downgrade':
        await set_state(args.appname, downdiff)
    else:
        print("upgrade diff looks like")
        print(updiff)
        print("downgrade diff looks like")
        print(downdiff)


if __name__ == '__main__':
    args = parser.parse_args()
    asyncio.run(
        update_db(args.appname,
                  args.schema,
                  args.table,
                  args.action)
    )
