# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0

import os
import yaml
import logging
import logging.config


_cfg = {}
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
            ])
        )


def load_yml(abspath, cache=False):
    """Load a yml file and optionally cache
    it into a package level cache.

    Args:
        abspath (str): path to yml file
        cache (bool): if True, don't re-parse
                      if called again
    """
    r = {}
    if cache:
        r = _cfg.get(abspath, {})
        if r:
            return r
    try:
        with open(abspath, 'r') as f:
            r = yaml.safe_load(f.read())
    except FileNotFoundError as e:
        _log.error(f"file not found: {abspath}")
    if cache:
        _cfg[abspath] = r
    return r


class cfg(Log):
    pass
cfg = cfg()


from sprout.cli.init_db import init_db, db_pool
from sprout.runner import Runner
