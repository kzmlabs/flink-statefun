# SPDX-License-Identifier: Apache-2.0
# Copyright 2014 The Apache Software Foundation

import abc
import typing
from datetime import timedelta

from statefun.core import SdkAddress
from statefun.messages import Message, EgressMessage


class Context(abc.ABC):
    __slots__ = ()

    @property
    @abc.abstractmethod
    def address(self) -> SdkAddress:
        """

        :return: the address of the currently executing function. the address is of the form (typename, id)
        """
        pass

    @property
    @abc.abstractmethod
    def storage(self):
        """

        :return: the address scoped storage.
        """
        pass

    @property
    @abc.abstractmethod
    def caller(self) -> typing.Union[None, SdkAddress]:
        """

        :return: the address of the caller or None if this function was triggered by the ingress.
        """
        pass

    def send(self, message: Message):
        """
        Send a message to a function.

        :param message: a message to send.
        """
        pass

    def send_after(self, duration: timedelta, message: Message, cancellation_token: str = ""):
        """
        Send a message to a target function after a specified delay.

        :param duration: the amount of time to wait before sending this message out.
        :param message: the message to send.
        :param cancellation_token: an optional cancellation token to associate with this message.
        """
        pass

    def cancel_delayed_message(self, cancellation_token: str):
        """
        Cancel a delayed message (message that was sent using send_after) with a given token.

        Please note that this is a best-effort operation, since the message might have been already delivered.
        If the message was delivered, this is a no-op operation.
        """
        pass

    def send_egress(self, message: EgressMessage):
        """
        Send a message to an egress.

        :param message: the EgressMessage to send.
        """
        pass
