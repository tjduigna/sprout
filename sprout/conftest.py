# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0
"""Provide a pytest fixture for an in-memory
database against which test can be run."""

#import pytest
#import aiosqlite
#
#import sprout
#
#@pytest.fixture
#def mockdb(event_loop):
#    con = await aiosqlite.connect("test.db", loop=event_loop)
#    cur = await con.cursor()
#
