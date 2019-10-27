# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0

import os
import asyncio

import asyncpg
from tortoise import Tortoise

import sprout

class Runner(sprout.Log):
    """An object-oriented interface
    to the sprout utilities.
    
    Args:
        cfg (str,dict): config or path to it
        env (str): key in cfg if it's nested
        rc (str): path to secrets yaml file
        app (str): app name
    """
    _loop = asyncio.get_event_loop()

    def _init_cfg(self, cfg):
        if isinstance(cfg, str):
            cfg = sprout.load_yml(cfg)
        if not isinstance(cfg, dict) or not cfg:
            raise Exception("cfg not understood")
        if self.env is not None:
            cfg = cfg[self.env]
        for key in ['host', 'port', 'database']:
            if key not in cfg:
                raise Exception(f"'{key}' not found in cfg")
        if 'username' not in cfg:
            raise Exception("'username' not found in cfg")
        if self.rc is not None:
            cfg.update(sprout.load_yml(self.rc))
        return cfg

    def __init__(self, cfg, env=None, rc=None,
                 app=None, schemas=None):
        self.env = env
        self.rc = rc
        self._cfg = self._init_cfg(cfg)
        self.app = app
        if schemas is None:
            schemas = []
        self.schemas = schemas

    def db_str(self, dbname=None, schema=None):
        c = self._cfg
        dbname = dbname or c['database']
        auth = f"{c['username']}:{c['password']}"
        url = f"{c['host']}:{c['port']}"
        base = f"{c['driver']}://{auth}@{url}"
        if schema is not None:
            return f"{base}/{dbname}?schema={schema}"
        return f"{base}/{dbname}"

    async def _create_database(self):
        if self.app is None:
            self.log.error("has no app")
            return
        con = await asyncpg.connect(self.db_str(dbname='postgres'))
        try:
            await con.execute(f"create database {self.app};")
        except asyncpg.exceptions.DuplicateDatabaseError:
            sprout.cfg.log.info(f"database {self.app} exists")
        finally:
            await con.close()

    async def _create_schemas(self):
        if not self.app or not self.schemas:
            self.log.error("either has no app or schemas")
            return
        con = await asyncpg.connect(self.db_str())
        for name in self.schemas:
            try:
                await con.execute(f"create schema {name};")
            except asyncpg.exceptions.DuplicateSchemaError:
                sprout.cfg.log.info(f"schema {name} exists")
        await con.close()

    async def _init_schemas(self):
        await self._create_database()
        for schema in self.schemas:
            await self._create_schemas()
            await Tortoise.init(
                db_url=self.db_str(schema=schema),
                modules={'models': [f'{self.app}.orm.{schema}']}
            )
            await Tortoise.generate_schemas()
            self.log.info(f"'{schema}' ready")

    async def _init_db_pool(self):
        c = self._cfg.copy()
        c['user'] = c.pop('username')
        c.pop('driver')
        if self.app is None:
            self.log.error("no app name provided")
            return
        c['database'] = self.app
        pw = c.pop('password')
        self.log.info(f"db_pool: {c}")
        c['password'] = pw
        pool = await asyncpg.create_pool(**c)
        return pool

    def create_database(self, app=None):
        self.app = app or self.app
        self._loop.run_until_complete(self._create_database())

    def create_schemas(self, app=None, schemas=None):
        self.app = app or self.app
        self.schemas = schemas or self.schemas
        self._loop.run_until_complete(self._create_schemas())

    def init_schemas(self, app=None, schemas=None):
        self.app = app or self.app
        self.schemas = schemas or self.schemas
        self._loop.run_until_complete(self._init_schemas())

    def init_db_pool(self, app=None):
        self.app = app or self.app
        pool = self._loop.run_until_complete(self._init_db_pool())
        return pool


