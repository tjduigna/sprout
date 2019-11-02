# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0

from tortoise.models import Model
from tortoise import fields

class User(Model):
    pk = 'id'
    ui = ['id', 'name', 'contact', 'profile']
    id = fields.IntField(pk=True)
    name = fields.TextField()
    pw = fields.TextField()
    salt = fields.ForeignKey('models.Salt', related_name='salts')
    contact = fields.TextField()
    profile = fields.TextField()

    def __str__(self):
        return f"{self.name}"
