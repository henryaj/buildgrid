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

        self._bot_ids = {}
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

        try:
            self._check_bot_ids(bot_id)
        except InvalidArgumentError:
            pass

        # Bot session name, selected by the server
        name = str(uuid.uuid4())
        bot_session.name = name

        self._bot_ids[name] = bot_id
        self.logger.info("Created bot session name={} with bot_id={}".format(name, bot_id))
        return bot_session

    def update_bot_session(self, name, bot_session):
        """ Client updates the server. Any changes in state to the Lease should be
        registered server side. Assigns available leases with work.
        """
        self.logger.debug("Updating bot session name={}".format(name))
        self._check_bot_ids(bot_session.bot_id, name)

        leases = [self._scheduler.update_lease(lease) for lease in bot_session.leases]

        del bot_session.leases[:]
        bot_session.leases.extend(leases)

        return bot_session

    def _check_bot_ids(self, bot_id, name = None):
        """ Checks the ID and the name of the bot.
        """
        if name is not None:
            _bot_id = self._bot_ids.get(name)
            if _bot_id is None:
                raise InvalidArgumentError('Name not registered on server: {}'.format(name))
            elif _bot_id != bot_id:
                self._close_bot_session(name)
                raise InvalidArgumentError('Bot id invalid. ID sent: {} with name: {}. ID registered: {} with name: {}'.format(bot_id, name, _bot_id, _name))

        else:
             for _name, _bot_id in self._bot_ids.items():
                 if bot_id == _bot_id:
                     self._close_bot_session(_name)
                     raise InvalidArgumentError('Bot id already registered. ID sent: {}. Id registered: {} with name: {}'.format(bot_id, _bot_id, _name))

    def _close_bot_session(self, name):
        """ Before removing the session, close any leases and
        requeue with high priority.
        """
        bot_id = self._bot_ids.get(name)

        if bot_id is None:
            raise InvalidArgumentError("Bot id does not exist: {}".format(name))

        self.logger.debug("Attempting to close {} with name: {}".format(bot_id, name))
        self._scheduler.retry_job(name)
        self.logger.debug("Closing bot session: {}".format(name))
        self._bot_ids.pop(name)
        self.logger.info("Closed bot {} with name: {}".format(bot_id, name))
