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
BotsInterface
=================

Instance of the Remote Workers interface.
"""

import logging
import uuid

from buildgrid._exceptions import InvalidArgumentError, OutOfSyncError

from ..job import LeaseState


class BotsInterface:

    def __init__(self, scheduler):
        self.__logger = logging.getLogger(__name__)

        self._bot_ids = {}
        self._bot_sessions = {}
        self._scheduler = scheduler

    def register_instance_with_server(self, instance_name, server):
        server.add_bots_interface(self, instance_name)

    def create_bot_session(self, parent, bot_session):
        """ Creates a new bot session. Server should assign a unique
        name to the session. If a bot with the same bot id tries to
        register with the service, the old one should be closed along
        with all its jobs.
        """

        bot_id = bot_session.bot_id

        if bot_id == "":
            raise InvalidArgumentError("bot_id needs to be set by client")

        try:
            self._check_bot_ids(bot_id)
        except InvalidArgumentError:
            pass

        # Bot session name, selected by the server
        name = "{}/{}".format(parent, str(uuid.uuid4()))

        bot_session.name = name

        self._bot_ids[name] = bot_id
        self._bot_sessions[name] = bot_session
        self.__logger.info("Created bot session name=[%s] with bot_id=[%s]", name, bot_id)

        # TODO: Send worker capabilities to the scheduler!
        leases = self._scheduler.request_job_leases({})
        if leases:
            bot_session.leases.extend(leases)

        return bot_session

    def update_bot_session(self, name, bot_session):
        """ Client updates the server. Any changes in state to the Lease should be
        registered server side. Assigns available leases with work.
        """
        self.__logger.debug("Updating bot session name=[%s]", name)
        self._check_bot_ids(bot_session.bot_id, name)

        leases = filter(None, [self.check_states(lease) for lease in bot_session.leases])

        del bot_session.leases[:]
        bot_session.leases.extend(leases)

        # TODO: Send worker capabilities to the scheduler!
        if not bot_session.leases:
            leases = self._scheduler.request_job_leases({})
            if leases:
                bot_session.leases.extend(leases)

        self._bot_sessions[name] = bot_session
        return bot_session

    def check_states(self, client_lease):
        """ Edge detector for states
        """
        # TODO: Handle cancelled states
        try:
            server_lease = self._scheduler.get_job_lease(client_lease.id)
        except KeyError:
            raise InvalidArgumentError("Lease not found on server: [{}]".format(client_lease))

        server_state = LeaseState(server_lease.state)
        client_state = LeaseState(client_lease.state)

        if server_state == LeaseState.PENDING:

            if client_state == LeaseState.ACTIVE:
                self._scheduler.update_job_lease_state(client_lease.id,
                                                       LeaseState.ACTIVE)
            elif client_state == LeaseState.COMPLETED:
                # TODO: Lease was rejected
                raise NotImplementedError("'Not Accepted' is unsupported")
            else:
                raise OutOfSyncError("Server lease: [{}]. Client lease: [{}]".format(server_lease, client_lease))

        elif server_state == LeaseState.ACTIVE:

            if client_state == LeaseState.ACTIVE:
                pass

            elif client_state == LeaseState.COMPLETED:
                self._scheduler.update_job_lease_state(client_lease.id,
                                                       LeaseState.COMPLETED,
                                                       lease_status=client_lease.status,
                                                       lease_result=client_lease.result)
                return None

            else:
                raise OutOfSyncError("Server lease: [{}]. Client lease: [{}]".format(server_lease, client_lease))

        elif server_state == LeaseState.COMPLETED:
            raise OutOfSyncError("Server lease: [{}]. Client lease: [{}]".format(server_lease, client_lease))

        elif server_state == LeaseState.CANCELLED:
            raise NotImplementedError("Cancelled states not supported yet")

        else:
            # Sould never get here
            raise OutOfSyncError("State now allowed: {}".format(server_state))

        return client_lease

    def _check_bot_ids(self, bot_id, name=None):
        """ Checks the ID and the name of the bot.
        """
        if name is not None:
            _bot_id = self._bot_ids.get(name)
            if _bot_id is None:
                raise InvalidArgumentError('Name not registered on server: [{}]'.format(name))
            elif _bot_id != bot_id:
                self._close_bot_session(name)
                raise InvalidArgumentError(
                    'Bot id invalid. ID sent: [{}] with name: [{}].'
                    'ID registered: [{}] for that name'.format(bot_id, name, _bot_id))
        else:
            for _name, _bot_id in self._bot_ids.items():
                if bot_id == _bot_id:
                    self._close_bot_session(_name)
                    raise InvalidArgumentError(
                        'Bot id already registered. ID sent: [{}].'
                        'Id registered: [{}] with name: [{}]'.format(bot_id, _bot_id, _name))

    def _close_bot_session(self, name):
        """ Before removing the session, close any leases and
        requeue with high priority.
        """
        bot_id = self._bot_ids.get(name)

        if bot_id is None:
            raise InvalidArgumentError("Bot id does not exist: [{}]".format(name))

        self.__logger.debug("Attempting to close [%s] with name: [%s]", bot_id, name)
        for lease in self._bot_sessions[name].leases:
            if lease.state != LeaseState.COMPLETED.value:
                # TODO: Be wary here, may need to handle rejected leases in future
                self._scheduler.retry_job(lease.id)

        self.__logger.debug("Closing bot session: [%s]", name)
        self._bot_ids.pop(name)
        self.__logger.info("Closed bot [%s] with name: [%s]", bot_id, name)
