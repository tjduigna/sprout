# -*- coding: utf-8 -*-
# Copyright 2019, Sprout Development Team
# Distributed under the terms of the Apache License 2.0

import hashlib
import secrets
import string
import base64
import hmac

_allowed = (string.digits +
            string.ascii_lowercase +
            string.ascii_uppercase)

def get_random_str():
    length = 12
    return ''.join(secrets.choice(_allowed)
                   for _ in range(length))

def to_bytes(s):
    if isinstance(s, bytes):
        return s
    return s.encode('utf-8', 'strict')

def from_bytes(b):
    if isinstance(b, str):
        return b
    return b.decode('utf-8', 'strict')

def salted_hmac(key, value, secret):
    key = to_bytes(key)
    sec = to_bytes(secret)
    key = hashlib.sha1(key + secret).digest()
    return hmac.new(key, msg=to_bytes(value),
                    digestmethod=hashlib.sha1)

def pbkdf2(password, salt, iterations):
    digest = hashlib.sha256
    pw = to_bytes(password)
    salt = to_bytes(salt)
    return hashlib.pbkdf2_hmac(digest().name, pw,
                               salt, iterations, None)

def compare(v1, v2):
    return secrets.compare_digest(to_bytes(v1),
                                  to_bytes(v2))

def mask_hash(hash_):
    return hash_[:show] + '*' * len(hash_[show:])

class PassHash:
    algorithm = 'pbkdf2_sha256'

    def encode(self, password, salt, iterations=None):
        if password is None or salt is None or '$' in salt:
            raise Exception("malformed password or salt")
        iterations = iterations or 20000
        hash_ = pbkdf2(password, salt, iterations)
        hash_ = base64.b64encode(hash_).decode('ascii').strip()
        return f"{self.algorithm}${iterations}${salt}${hash_}"

    def verify(self, password, encoded):
        algorithm, iterations, salt, hash_ = encoded.split('$', 3)
        if algorithm != self.algorithm:
            raise Exception("algorithm mismatch")
        other = self.encode(password, salt, int(iterations))
        return compare(encoded, other)

    def summary(self, encoded):
        algorithm, iterations, salt, hash_ = encoded.split('$', 3)
        if algorithm != self.algorithm:
            raise Exception("algorithm mismatch")
        return {
            'algorithm': algorithm,
            'iterations': iterations,
            'salt': mask_hash(salt),
            'hash': mask_hash(hash_)
        }

