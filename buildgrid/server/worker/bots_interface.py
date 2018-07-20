# Copyright (C) 2018 Codethink Limited
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
# Authors:
#        Finn Ball <finn.ball@codethink.co.uk>

"""
BotsInterface
=================

Instance of the Remote Workers interface.
"""

import logging
import uuid

from ._exceptions import InvalidArgumentError, OutofSyncError
from ..job import LeaseState
from ..scheduler import Scheduler

class BotsInterface():

    def __init__(self, scheduler):
        self.logger = logging.getLogger(__name__)

        self._bots = {}
        self._scheduler = scheduler

    def create_bot_session(self, parent, bot_session):
        """ Creates a new bot session. Server should assign a unique
        name to the session. If a bot with the same bot id tries to
        register with the service, the old one should be closed along
        with all its jobs.
        """

        bot_id = bot_session.bot_id

        if bot_id == "":
            raise InvalidArgumentError("bot_id needs to be set by client")

        self._check_bot_ids(bot_id)

        # Bot session name, selected by the server
        name = str(uuid.uuid4())
        bot_session.name = name

        self._bots[name] = bot_session
        self.logger.info("Created bot session name={} with bot_id={}".format(name, bot_id))
        return bot_session

    def update_bot_session(self, name, bot_session):
        """ Client updates the server. Any changes in state to the Lease should be
        registered server side. Assigns available leases with work.
        """
        self.logger.debug("Updating bot session name={}".format(name))
        bot = self._bots.get(name)

        if bot == None:
            self.logger.warn("Update received for {} but not found on server ({}).".format(name,
                                                                                           bot_session.bot_id))
            raise InvalidArgumentError("Bot with name={} is not registered on server.".format(name))
        else:
            leases_server = bot.leases

        # Close any zombies
        if not self._check_bot_ids(name, bot_session):
            raise InvalidArgumentError("Bot with name={} has incorrect bot_id={}.".format(name,
                                                                                          bot_session.bot_id))

        leases_client = bot_session.leases
        if len(leases_client) != len(leases_server):
            self._close_bot_session(name)
            raise OutofSyncError("Number of leases in server and client not same."+\
                                 "Closed bot session: {}".format(name)+\
                                 "Client: {}\nServer: {}".format(len(leases_client), len(leases_server)))

        leases_client = [self._check_lease(lease) for lease in leases_client]

        del bot_session.leases[:]
        bot_session.leases.extend(leases_client)

        self._bots[name] = bot_session
        return bot_session

    def _check_bot_ids(self, bot_id, name = None):
        ''' Generate a list of all the bots that are reporting with this id but
        not this name. Per the spec, any bot that is reporting an ID that
        does not match the name we have file for them should not be given any
        work. Returns False if the check fails and a bot_session is closed.'''

        for _name, bot in list(self._bots.items()):
            if bot.bot_id == bot_id and _name != name:
                self.logger.warn("Duplicate bot_id provided of {}: this is registered under names {} and {}. Closing session with {}."
                                 .format(bot_id, name, _name, _name))
                self._close_bot_session(_name)
                return False
        return True

    def _check_lease(self, lease):
        """ Checks status of lease
        """
        state = lease.state

        if state   == LeaseState.LEASE_STATE_UNSPECIFIED.value:
            return self._get_pending_action(lease)

        elif state == LeaseState.PENDING.value:
            return lease

        elif state == LeaseState.ACTIVE.value:
            return lease

        elif state == LeaseState.COMPLETED.value:
            name = lease.assignment
            result = lease.inline_assignment
            self._scheduler.job_complete(name, result)

            return self._get_pending_action(lease)

        elif state == LeaseState.CANCELLED.value:
            # TODO: Add cancelled message to result
            raise NotImplementedError

        else:
            raise InvalidArgumentError("Unknown state: {}".format(state))

    def _get_pending_action(self, lease):
        """ If actions are available, populates the lease and
        informs the execution service, else it returns the lease.
        """
        if len(self._scheduler.queue) > 0:
            job = self._scheduler.get_job()
            lease = job.get_lease()
        else:
            # Doc says not to use this sate.
            # Though if lease has completed, no need to process again
            # if there are no pending actions
            lease.state = LeaseState.LEASE_STATE_UNSPECIFIED.value

        return lease

    def _requeue_lease_if_applicable(self, lease):
        state = lease.state
        if state == LeaseState.PENDING.value or \
           state == LeaseState.ACTIVE.value:
            name = lease.assignment
            action = lease.inline_assignment
            self._scheduler.jobs[name].retry_job(name)

    def _close_bot_session(self, name):
        """ Before removing the session, close any leases and
        requeue with high priority.
        """
        bot = self._bots.get(name)

        if bot is None:
            raise InvalidArgumentError("Bot name does not exist: {}".format(name))

        self.logger.debug("Attempting to close {} with name: {}".format(bot.bot_id, name))
        try:
            for lease in bot.leases:
                self._requeue_lease_if_applicable(lease)
            self.logger.debug("Closing bot session: {}".format(name))
            self._bots.pop(name)
            self.logger.info("Closed bot {} with name: {}".format(bot.bot_id, name))
        except KeyError:
            raise InvalidArgumentError("Bot name does not exist: {}".format(name))
