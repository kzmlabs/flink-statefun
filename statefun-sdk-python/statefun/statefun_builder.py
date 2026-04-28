# SPDX-License-Identifier: Apache-2.0


import typing

from statefun.core import ValueSpec
from statefun.context import Context
from statefun.messages import Message
from statefun.storage import make_address_storage_spec, StorageSpec
import inspect


class StatefulFunction(object):
    __slots__ = ("fun", "storage_spec", "is_async")

    def __init__(self,
                 fun: typing.Callable[[Context, Message], None],
                 specs: StorageSpec,
                 is_async: bool):
        if fun is None:
            raise ValueError("function code is missing.")
        self.fun = fun
        if specs is None:
            raise ValueError("storage spec is missing.")
        self.storage_spec = specs
        self.is_async = is_async


class StatefulFunctions(object):
    __slots__ = ("_functions",)

    def __init__(self):
        self._functions = {}

    def register(self, typename: str, fun, specs: typing.Optional[typing.List[ValueSpec]] = None):
        """registers a StatefulFunction function instance, under the given namespace with the given function type. """
        if fun is None:
            raise ValueError("function instance must be provided")
        if not typename:
            raise ValueError("function typename must be provided")
        storage_spec = make_address_storage_spec(specs if specs else [])
        is_async = inspect.iscoroutinefunction(fun)
        sig = inspect.getfullargspec(fun)
        if len(sig.args) != 2:
            raise ValueError(
                f"The registered function {typename} does not expect a context and a message but rather {sig.args}.")
        self._functions[typename] = StatefulFunction(fun=fun, specs=storage_spec, is_async=is_async)

    def bind(self, typename, specs: typing.List[ValueSpec] = None):
        """wraps a StatefulFunction instance with a given namespace and type.
           for example:
            s = StatefulFunctions()

            @s.define("com.foo.bar/greeter")
            def greeter(context, message):
                print("Hi there")

            This would add an invokable stateful function that can accept messages
            sent to "com.foo.bar/greeter".
         """

        def wrapper(function):
            self.register(typename, function, specs)
            return function

        return wrapper

    def for_typename(self, typename: str) -> StatefulFunction:
        return self._functions[typename]
