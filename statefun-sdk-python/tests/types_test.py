# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation

import unittest

import statefun
from statefun.utils import to_typed_value


class TypeNameTestCase(unittest.TestCase):

    def assertRoundTrip(self, tpe, value):
        serializer = tpe.serializer()
        out = serializer.serialize(value)
        got = serializer.deserialize(out)
        self.assertEqual(got, value)

    def test_built_ins(self):
        self.assertRoundTrip(statefun.BoolType, True)
        self.assertRoundTrip(statefun.IntType, 0)
        self.assertRoundTrip(statefun.FloatType, float(0.5))
        self.assertRoundTrip(statefun.DoubleType, 1e-20)
        self.assertRoundTrip(statefun.LongType, 1 << 45)
        self.assertRoundTrip(statefun.StringType, "hello world")

    def test_json_type(self):
        import json
        tpe = statefun.simple_type(typename="org.foo.bar/UserJson",
                                   serialize_fn=json.dumps,
                                   deserialize_fn=json.loads)

        self.assertRoundTrip(tpe, {"name": "bob", "last": "mop"})

    def test_message(self):
        typed_value = to_typed_value(statefun.StringType, "hello world")
        msg = statefun.Message(target_typename="foo/bar", target_id="1", typed_value=typed_value)

        self.assertTrue(msg.is_string())
        self.assertEqual(msg.as_string(), "hello world")
