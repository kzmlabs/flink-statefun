# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation


import unittest

from statefun.core import parse_typename


class TypeNameTestCase(unittest.TestCase):

    def test_example(self):
        namespace, name = parse_typename("foo/bar")

        self.assertEqual(namespace, "foo")
        self.assertEqual(name, "bar")

    def test_no_namespace(self):
        with self.assertRaises(ValueError):
            parse_typename("/bar")

    def test_no_name(self):
        with self.assertRaises(ValueError):
            parse_typename("n/")

    def test_no_namespace_and_name(self):
        with self.assertRaises(ValueError):
            parse_typename("/")

    def test_empty_string(self):
        with self.assertRaises(ValueError):
            parse_typename("")
