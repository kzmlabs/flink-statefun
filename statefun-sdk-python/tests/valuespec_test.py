# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation

import unittest

from statefun import IntType
from statefun.storage import *
from datetime import timedelta


class ValueSpecTestCase(unittest.TestCase):

    def test_example(self):
        a = ValueSpec(name="a", type=IntType)
        self.assertEqual(a.name, "a")
        self.assertEqual(a.type, IntType)
        self.assertFalse(a.after_write)
        self.assertFalse(a.after_call)

    def test_expire_after_access(self):
        a = ValueSpec(name="a", type=IntType, expire_after_call=timedelta(seconds=1))
        self.assertTrue(a.after_call)
        self.assertEqual(a.duration, 1000)

        self.assertFalse(a.after_write)

    def test_expire_after_write(self):
        a = ValueSpec(name="a", type=IntType, expire_after_write=timedelta(seconds=1))
        self.assertTrue(a.after_write)
        self.assertEqual(a.duration, 1000)

        self.assertFalse(a.after_call)

    def test_illegal_name(self):
        with self.assertRaises(ValueError):
            ValueSpec(name="-a", type=IntType, expire_after_call=timedelta(1))
        with self.assertRaises(ValueError):
            ValueSpec(name="def", type=IntType, expire_after_call=timedelta(1))
