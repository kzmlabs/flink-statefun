# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation

import unittest

import statefun

from statefun import message_builder


class MessageTestCase(unittest.TestCase):

    def test_example(self):
        m = message_builder(target_typename="foo/bar", target_id="a", int_value=1)

        self.assertTrue(m.is_int())
        self.assertEqual(m.as_int(), 1)

    def test_with_type(self):
        m = message_builder(target_typename="foo/bar", target_id="a", value=5.0, value_type=statefun.FloatType)
        self.assertTrue(m.is_float())
        self.assertEqual(m.as_float(), 5.0)

    def test_kafka_egress(self):
        record = statefun.kafka_egress_message(typename="foo/bar", topic="topic", value=1337420)

        self.assertEqual(record.typed_value.typename, "type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord")
        self.assertTrue(record.typed_value.has_value)
