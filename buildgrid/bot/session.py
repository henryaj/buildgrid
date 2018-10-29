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

# Disable broad exception catch
# pylint: disable=broad-except


"""
Bot Session
===========

Allows connections
"""
import asyncio
import logging
import platform
import uuid

import grpc

from buildgrid._enums import BotStatus, LeaseState
from buildgrid._protos.google.rpc import code_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2, worker_pb2
from buildgrid._exceptions import BotError


class BotSession:
    def __init__(self, parent, interface, worker):
        """ Unique bot ID within the farm used to identify this bot
        Needs to be human readable.
        All prior sessions with bot_id of same ID are invalidated.
        If a bot attempts to update an invalid session, it must be rejected and
        may be put in quarantine.
        """
        self.__logger = logging.getLogger(__name__)

        self._context = None

        self._worker = worker
        self._interface = interface
        self._leases = {}
        self._parent = parent
        self._status = BotStatus.OK.value

        self.__bot_id = '{}.{}'.format(parent, platform.node())
        self.__name = None

    @property
    def bot_id(self):
        return self.__bot_id

    def create_bot_session(self, work, context=None):
        self.__logger.debug("Creating bot session")
        self._work = work
        self._context = context

        session = self._interface.create_bot_session(self._parent, self.get_pb2())
        self.__name = session.name

        self.__logger.info("Created bot session with name: [%s]", self._name)

        for lease in session.leases:
            self._update_lease_from_server(lease)

    def update_bot_session(self):
        self.__logger.debug("Updating bot session: [%s]", self._bot_id)
        session = self._interface.update_bot_session(self.get_pb2())
        for k, v in list(self._leases.items()):
            if v.state == LeaseState.COMPLETED.value:
                del self._leases[k]

        for lease in session.leases:
            self._update_lease_from_server(lease)

    def get_pb2(self):
        leases = list(self._leases.values())
        if not leases:
            leases = None

        return bots_pb2.BotSession(worker=self._worker.get_pb2(),
                                   status=self._status,
                                   leases=leases,
                                   bot_id=self.__bot_id,
                                   name=self.__name)

    def lease_completed(self, lease):
        lease.state = LeaseState.COMPLETED.value
        self._leases[lease.id] = lease

    def _update_lease_from_server(self, lease):
        """
        State machine for any recieved updates to the leases.
        """
        # TODO: Compare with previous state of lease
        if lease.state == LeaseState.PENDING.value:
            lease.state = LeaseState.ACTIVE.value
            self._leases[lease.id] = lease
            self.update_bot_session()
            asyncio.ensure_future(self.create_work(lease))

    async def create_work(self, lease):
        self.__logger.debug("Work created: [%s]", lease.id)
        loop = asyncio.get_event_loop()

        try:
            lease = await loop.run_in_executor(None, self._work, self._context, lease)

        except grpc.RpcError as e:
            self.__logger.error(e)
            lease.status.CopyFrom(e.code())

        except BotError as e:
            self.__logger.error(e)
            lease.status.code = code_pb2.INTERNAL

        except Exception as e:
            self.__logger.error(e)
            lease.status.code = code_pb2.INTERNAL

        self.__logger.debug("Work complete: [%s]", lease.id)
        self.lease_completed(lease)
