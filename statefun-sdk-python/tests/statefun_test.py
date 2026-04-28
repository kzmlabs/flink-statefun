# SPDX-License-Identifier: Apache-2.0


import unittest
from statefun import StatefulFunctions, ValueSpec, IntType, StringType


# noinspection PyUnusedLocal
class StatefulFunctionsTestCase(unittest.TestCase):

    def test_example(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename="org.foo/greeter",
            specs=[ValueSpec(name='seen_count', type=IntType)])
        def greeter(context, message):
            pass

        fun = functions.for_typename("org.foo/greeter")
        self.assertFalse(fun.is_async)
        self.assertIsNotNone(fun.storage_spec)

    def test_async(self):
        functions = StatefulFunctions()

        @functions.bind(
            typename="org.foo/greeter",
            specs=[ValueSpec(name='seen_count', type=IntType)])
        async def greeter(context, message):
            pass

        fun = functions.for_typename("org.foo/greeter")
        self.assertTrue(fun.is_async)
        self.assertIsNotNone(fun.storage_spec)

    def test_state_spec(self):
        functions = StatefulFunctions()

        foo = ValueSpec(name='foo', type=IntType)
        bar = ValueSpec(name='bar', type=StringType)

        @functions.bind(typename="org.foo/greeter", specs=[foo, bar])
        def greeter(context, message):
            pass

        fun = functions.for_typename("org.foo/greeter")
        self.assertListEqual(fun.storage_spec.specs, [foo, bar])

    def test_stateless(self):
        functions = StatefulFunctions()

        @functions.bind(typename="org.foo/greeter")
        def greeter(context, message):
            pass

        fun = functions.for_typename("org.foo/greeter")
        self.assertListEqual(fun.storage_spec.specs, [])

    def test_duplicate_state(self):
        functions = StatefulFunctions()

        with self.assertRaises(ValueError):
            @functions.bind(
                typename="org.foo/greeter",
                specs=[ValueSpec(name="bar", type=IntType), ValueSpec(name="bar", type=IntType)])
            def foo(context, message):
                pass

    def test_wrong_signature(self):
        functions = StatefulFunctions()

        with self.assertRaises(ValueError):
            @functions.bind(
                typename="org.foo/greeter",
                specs=[ValueSpec(name="bar", type=IntType)])
            def foo(message):  # missing context
                pass
