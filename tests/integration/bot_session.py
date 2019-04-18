# Copyright (C) 2018 Bloomberg LP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# pylint: disable=redefined-outer-name


import asyncio

import grpc
import pytest

from buildgrid._enums import LeaseState
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid.bot.hardware.worker import Worker
from buildgrid.bot.hardware.interface import HardwareInterface
from buildgrid.bot.session import BotSession
from buildgrid.bot.interface import BotInterface

from ..utils.utils import run_in_subprocess
from ..utils.bots_interface import serve_bots_interface


INSTANCES = ['', 'instance']
TIMEOUT = 5


# Use subprocess to avoid creation of gRPC threads in main process
# See https://github.com/grpc/grpc/blob/master/doc/fork_support.md
# Multiprocessing uses pickle which protobufs don't work with
# Workaround wrapper to send messages as strings
class ServerInterface:

    def __init__(self, remote):
        self.__remote = remote

    def create_bot_session(self, parent, bot_session):

        def __create_bot_session(queue, remote, parent, string_bot_session):
            bot_session = bots_pb2.BotSession()
            bot_session.ParseFromString(string_bot_session)

            interface = BotInterface(grpc.insecure_channel(remote), TIMEOUT, TIMEOUT)

            result = interface.create_bot_session(parent, bot_session)
            queue.put(result.SerializeToString())

        string_bot_session = bot_session.SerializeToString()
        result = run_in_subprocess(__create_bot_session,
                                   self.__remote, parent, string_bot_session)

        bot_session = bots_pb2.BotSession()
        bot_session.ParseFromString(result)
        return bot_session

    def update_bot_session(self, bot_session, update_mask=None):

        def __update_bot_session(queue, remote, string_bot_session, update_mask):
            bot_session = bots_pb2.BotSession()
            bot_session.ParseFromString(string_bot_session)

            interface = BotInterface(grpc.insecure_channel(remote), TIMEOUT, TIMEOUT)

            result = interface.update_bot_session(bot_session, update_mask)
            queue.put(result.SerializeToString())

        string_bot_session = bot_session.SerializeToString()
        result = run_in_subprocess(__update_bot_session,
                                   self.__remote, string_bot_session, update_mask)

        bot_session = bots_pb2.BotSession()
        bot_session.ParseFromString(result)
        return bot_session


@pytest.mark.parametrize('instance', INSTANCES)
def test_create_bot_session(instance):

    with serve_bots_interface([instance]) as server:
        interface = ServerInterface(server.remote)
        hardware_interface = HardwareInterface(Worker())
        session = BotSession(instance, interface, hardware_interface, None)
        session.create_bot_session()
        assert session.get_pb2() == server.get_bot_session()


@pytest.mark.parametrize('instance', INSTANCES)
def test_update_bot_session(instance):

    with serve_bots_interface([instance]) as server:
        interface = ServerInterface(server.remote)
        hardware_interface = HardwareInterface(Worker())
        session = BotSession(instance, interface, hardware_interface, None)
        session.create_bot_session()
        assert session.get_pb2() == server.get_bot_session()
        session.update_bot_session()
        assert session.get_pb2() == server.get_bot_session()


@pytest.mark.parametrize('instance', INSTANCES)
def test_create_bot_session_with_work(instance):

    def __work(lease, context, event):
        return lease

    with serve_bots_interface([instance]) as server:
        interface = ServerInterface(server.remote)
        hardware_interface = HardwareInterface(Worker())
        session = BotSession(instance, interface, hardware_interface, __work)
        server.inject_work()
        session.create_bot_session()

        assert len(session.get_pb2().leases) == 1

        loop = asyncio.get_event_loop()
        for task in asyncio.Task.all_tasks():
            loop.run_until_complete(task)

        assert session.get_pb2().leases[0].state == LeaseState.COMPLETED.value


@pytest.mark.parametrize('instance', INSTANCES)
def test_update_bot_session_with_work(instance):

    def __work(lease, context, event):
        return lease

    with serve_bots_interface([instance]) as server:
        interface = ServerInterface(server.remote)
        hardware_interface = HardwareInterface(Worker())
        session = BotSession(instance, interface, hardware_interface, __work)
        session.create_bot_session()
        server.inject_work()
        session.update_bot_session()

        assert len(session.get_pb2().leases) == 1

        loop = asyncio.get_event_loop()
        for task in asyncio.Task.all_tasks():
            loop.run_until_complete(task)

        assert session.get_pb2().leases[0].state == LeaseState.COMPLETED.value


@pytest.mark.parametrize('instance', INSTANCES)
def test_cancel_leases(instance):

    def __work(lease, context, cancel_event):
        # while not cancel_event.is_set():

        return lease

    with serve_bots_interface([instance]) as server:
        interface = ServerInterface(server.remote)
        hardware_interface = HardwareInterface(Worker())
        session = BotSession(instance, interface, hardware_interface, __work)

        lease = bots_pb2.Lease()
        lease.state = LeaseState.PENDING.value
        lease.id = 'foo'
        server.inject_work(lease)
        session.create_bot_session()

        leases_pb2 = session.get_pb2().leases
        assert len(leases_pb2) == 1
        assert leases_pb2[0].state == LeaseState.ACTIVE.value

        server.cancel_lease(leases_pb2[0].id)
        session.update_bot_session()
        assert len(session.get_pb2().leases) == 1

        loop = asyncio.get_event_loop()
        for task in asyncio.Task.all_tasks():
            try:
                loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass

        assert session.get_pb2().leases[0].state == LeaseState.CANCELLED.value
