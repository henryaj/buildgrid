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

from buildgrid._exceptions import InvalidArgumentError, NotFoundError
from buildgrid.settings import NETWORK_TIMEOUT

from ..job import LeaseState, BotStatus


class BotsInterface:

    def __init__(self, scheduler):
        self.__logger = logging.getLogger(__name__)
        # Turn on debug mode based on log verbosity level:
        self.__debug = self.__logger.getEffectiveLevel() <= logging.DEBUG

        self._scheduler = scheduler
        self._instance_name = None

        self._bot_ids = {}
        self._assigned_leases = {}

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @property
    def scheduler(self):
        return self._scheduler

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the bots interface with a given server."""
        if self._instance_name is None:
            server.add_bots_interface(self, instance_name)

            self._instance_name = instance_name

        else:
            raise AssertionError("Instance already registered")

    def create_bot_session(self, parent, bot_session):
        """ Creates a new bot session. Server should assign a unique
        name to the session. If a bot with the same bot id tries to
        register with the service, the old one should be closed along
        with all its jobs.
        """
        if not bot_session.bot_id:
            raise InvalidArgumentError("Bot's id must be set by client.")

        try:
            self._check_bot_ids(bot_session.bot_id)
        except InvalidArgumentError:
            pass

        # Bot session name, selected by the server
        name = "{}/{}".format(parent, str(uuid.uuid4()))
        bot_session.name = name

        self._bot_ids[name] = bot_session.bot_id

        # We want to keep a copy of lease ids we have assigned
        self._assigned_leases[name] = set()

        self._request_leases(bot_session)

        if self.__debug:
            self.__logger.info("Opened session name=[%s] for bot=[%s], leases=[%s]",
                               bot_session.name, bot_session.bot_id,
                               ",".join([lease.id[:8] for lease in bot_session.leases]))
        else:
            self.__logger.info("Opened session, name=[%s] for bot=[%s]",
                               bot_session.name, bot_session.bot_id)

        return bot_session

    def update_bot_session(self, name, bot_session, deadline=None):
        """ Client updates the server. Any changes in state to the Lease should be
        registered server side. Assigns available leases with work.
        """
        self._check_bot_ids(bot_session.bot_id, name)
        self._check_assigned_leases(bot_session)

        for lease in list(bot_session.leases):
            checked_lease = self._check_lease_state(lease)
            if not checked_lease:
                # TODO: Make sure we don't need this
                try:
                    self._assigned_leases[name].remove(lease.id)
                except KeyError:
                    pass
                try:
                    self._scheduler.delete_job_lease(lease.id)
                except NotFoundError:
                    # Job already dropped from scheduler
                    pass

                bot_session.leases.remove(lease)

        self._request_leases(bot_session, deadline)

        self.__logger.debug("Sending session update, name=[%s], for bot=[%s], leases=[%s]",
                            bot_session.name, bot_session.bot_id,
                            ",".join([lease.id[:8] for lease in bot_session.leases]))

        return bot_session

    # --- Private API ---

    def _request_leases(self, bot_session, deadline=None):
        # Only send one lease at a time currently.
        if bot_session.status == BotStatus.OK.value and not bot_session.leases:
            worker_capabilities = {}

            # TODO? Fail if there are no devices in the worker?
            if bot_session.worker.devices:
                # According to the spec:
                #   "The first device in the worker is the "primary device" -
                #   that is, the device running a bot and which is
                #   responsible for actually executing commands."
                primary_device = bot_session.worker.devices[0]

                for device_property in primary_device.properties:
                    if device_property.key not in worker_capabilities:
                        worker_capabilities[device_property.key] = set()
                    worker_capabilities[device_property.key].add(device_property.value)

            # If the client specified deadline is less than NETWORK_TIMEOUT,
            # the response shouldn't long poll for work.
            if deadline and (deadline > NETWORK_TIMEOUT):
                deadline = deadline - NETWORK_TIMEOUT
            else:
                deadline = None

            leases = self._scheduler.request_job_leases(
                worker_capabilities,
                timeout=deadline)

            if leases:
                for lease in leases:
                    self._assigned_leases[bot_session.name].add(lease.id)
                bot_session.leases.extend(leases)

    def _check_lease_state(self, lease):
        # careful here
        # should store bot name in scheduler
        lease_state = LeaseState(lease.state)

        # Lease has replied with cancelled, remove
        if lease_state == LeaseState.CANCELLED:
            return None

        try:
            if self._scheduler.get_job_lease_cancelled(lease.id):
                lease.state = LeaseState.CANCELLED.value
                return lease
        except KeyError:
            # Job does not exist, remove from bot.
            return None

        self._scheduler.update_job_lease_state(lease.id, lease)

        if lease_state == LeaseState.COMPLETED:
            return None

        return lease

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

    def _check_assigned_leases(self, bot_session):
        session_lease_ids = []

        for lease in bot_session.leases:
            session_lease_ids.append(lease.id)

        for lease_id in self._assigned_leases[bot_session.name]:
            if lease_id not in session_lease_ids:
                self.__logger.error("Assigned lease id=[%s],"
                                    " not found on bot with name=[%s] and id=[%s]."
                                    " Retrying job", lease_id, bot_session.name, bot_session.bot_id)
                try:
                    self._scheduler.retry_job_lease(lease_id)
                except NotFoundError:
                    pass

    def _close_bot_session(self, name):
        """ Before removing the session, close any leases and
        requeue with high priority.
        """
        bot_id = self._bot_ids.get(name)

        if bot_id is None:
            raise InvalidArgumentError("Bot id does not exist: [{}]".format(name))

        self.__logger.debug("Attempting to close [%s] with name: [%s]", bot_id, name)
        for lease_id in self._assigned_leases[name]:
            try:
                self._scheduler.retry_job_lease(lease_id)
            except NotFoundError:
                pass
        self._assigned_leases.pop(name)

        self.__logger.debug("Closing bot session: [%s]", name)
        self._bot_ids.pop(name)
        self.__logger.info("Closed bot [%s] with name: [%s]", bot_id, name)
