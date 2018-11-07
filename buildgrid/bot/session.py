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


"""
Bot Session
===========

Allows connections
"""
import logging
import platform

from buildgrid._enums import BotStatus, LeaseState
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.rpc import code_pb2

from buildgrid._exceptions import FailedPreconditionError

from .tenantmanager import TenantManager


class BotSession:
    def __init__(self, parent, bots_interface, hardware_interface, work, context=None):
        """ Unique bot ID within the farm used to identify this bot
        Needs to be human readable.
        All prior sessions with bot_id of same ID are invalidated.
        If a bot attempts to update an invalid session, it must be rejected and
        may be put in quarantine.
        """
        self.__logger = logging.getLogger(__name__)

        self._bots_interface = bots_interface
        self._hardware_interface = hardware_interface

        self._status = BotStatus.OK.value
        self._tenant_manager = TenantManager()

        self.__parent = parent
        self.__bot_id = '{}.{}'.format(parent, platform.node())
        self.__name = None

        self._work = work
        self._context = context

    @property
    def bot_id(self):
        return self.__bot_id

    def create_bot_session(self):
        self.__logger.debug("Creating bot session")

        session = self._bots_interface.create_bot_session(self.__parent, self.get_pb2())
        self.__name = session.name

        self.__logger.info("Created bot session with name: [%s]", self.__name)

        for lease in session.leases:
            self._register_lease(lease)

    def update_bot_session(self):
        self.__logger.debug("Updating bot session: [%s]", self.__bot_id)

        session = self._bots_interface.update_bot_session(self.get_pb2())
        server_ids = []

        for lease in session.leases:
            server_ids.append(lease.id)

            lease_state = LeaseState(lease.state)
            if lease_state == LeaseState.PENDING:
                self._register_lease(lease)

            elif lease_state == LeaseState.CANCELLED:
                self._tenant_manager.cancel_tenancy(lease.id)

        closed_lease_ids = [x for x in self._tenant_manager.get_lease_ids() if x not in server_ids]

        for lease_id in closed_lease_ids:
            self._tenant_manager.cancel_tenancy(lease_id)
            self._tenant_manager.remove_tenant(lease_id)

    def get_pb2(self):
        return bots_pb2.BotSession(worker=self._hardware_interface.get_worker_pb2(),
                                   status=self._status,
                                   leases=self._tenant_manager.get_leases(),
                                   bot_id=self.__bot_id,
                                   name=self.__name)

    def _register_lease(self, lease):
        lease_id = lease.id
        try:
            self._tenant_manager.create_tenancy(lease)

        except KeyError as e:
            self.__logger.error(e)

        else:
            try:
                self._hardware_interface.configure_hardware(lease.requirements)

            except FailedPreconditionError as e:
                self.__logger.error(e)
                self._tenant_manager.complete_lease(lease_id, status=code_pb2.FailedPreconditionError)

            else:
                self._tenant_manager.create_work(lease_id, self._work, self._context)
