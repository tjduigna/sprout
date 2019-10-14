# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0

import os
import yaml
import logging.config


_root = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(_root, 'conf', 'log.yml'), 'r') as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))
_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)


class Log:
    @property
    def log(self):
        return logging.getLogger(
            '.'.join([
                self.__module__,
                self.__class__.__name__
            ]))


class cfg(Log):

    def _load_cfg_fl(self, *path_parts):
        fp = os.path.join(_root, *path_parts)
        if not self._cfg.get(fp, None):
            try:
                with open(fp, 'r') as f:
                    self._cfg[fp] = yaml.safe_load(f.read())
            except FileNotFoundError as e:
                _log.error(f"file not found: {fp}")
        return self._cfg[fp]

    @property
    def environment(self):
        if self._environment is None:
            self._environment = os.environ.get('ENVIRONMENT', 'local')
        return self._environment

    @property
    def srv_opts(self):
        if self._srv_opts is None:
            opts = self._load_cfg_fl('conf', 'srv.yml')
            self._srv_opts = opts[self.environment]
        return self._srv_opts

    @property
    def db_opts(self):
        if self._db_opts is None:
            opts = self._load_cfg_fl('conf', 'db.yml')
            self._db_opts = opts[self.environment]
        return self._db_opts

    def db_str(self, dbname=None, schema=None):
        o = self.db_opts
        dbname = dbname or o['database']
        auth = f"{o['username']}:{o['password']}"
        url = f"{o['host']}:{o['port']}"
        if schema is not None:
            return f"{o['driver']}://{auth}@{url}/{dbname}?schema={schema}"
        return f"{o['driver']}://{auth}@{url}/{dbname}"

    def reset(self):
        self._cfg = {}
        self._db_opts = None
        self._srv_opts = None
        self._environment = None

    def __init__(self):
        self.reset()

cfg = cfg()

