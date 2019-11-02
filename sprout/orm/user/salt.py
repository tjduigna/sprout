# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0

from tortoise.models import Model
from tortoise import fields

class Salt(Model):
    pk = 'id'
    ui = []
    id = fields.IntField(pk=True)
    salt = fields.TextField()

    def __str__(self):
        return f"{self.id}"
