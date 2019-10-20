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
    """

# command line utility
parser = argparse.ArgumentParser(description="Dump application DB schema")
parser.add_argument('--appname', required=True, help="name of application")
parser.add_argument('--schema', required=True, help="schema where table resides")
parser.add_argument('--table', required=True, help="name of the table")

