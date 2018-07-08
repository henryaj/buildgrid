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


import grpc
import logging
import uuid
import os
import queue

from collections import namedtuple
from queue import Queue, PriorityQueue

from google.devtools.remoteworkers.v1test2 import bots_pb2, worker_pb2
from google.protobuf import any_pb2

from .._exceptions import InvalidArgumentError, OutofSyncError

class BotsInterface(object):

    def __init__(self):
        self.operation_queue = Queue(maxsize = 0)
        self._action_queue = PriorityQueue(maxsize = 0)
        self._bots = {}
        self.logger = logging.getLogger(__name__)

    def create_bot_session(self, parent, bot_session):
        """ Creates a new bot session. Server should assign a unique
        name to the session. If a bot with the same bot id tries to
        register with the service, the old one should be closed along
        with all its jobs.
        """

        # Bot session name, selected by the server
        name = str(uuid.uuid4())
        bot_id = bot_session.bot_id

        self.logger.debug("Creating bot session name={} bot_id={}".format(name, bot_id))
        if bot_id == None:
            raise InvalidArgumentError("bot_id needs to be set by client")

        for _name, _bot in list(self._bots.items()):
            if _bot.bot_id == bot_id:
                self.logger.warning("Bot id {} with already exists (name={}), closing previous bot session ({}).".format(bot_id, name, _name))
                self._close_bot_session(_name)

        bot_session.name = name
        self._bots[name] = bot_session
        self.logger.info("Created bot session name={} with bot_id={}".format(name, bot_id))
        return bot_session

    def update_bot_session(self, name, bot_session):
        """ Client updates the server. Any changes in state to the Lease should be
        registered server side. Assigns available leases with work.
        """
        self.logger.debug("Updating bot session name={}".format(name))
        if name not in self._bots:
            self.logger.warn("Update received for {} but not found on server ({}).".format(name,
                                                                                           bot_session.bot_id))
            raise InvalidArgumentError("Bot with name={} is not registered on server.".format(name))
        else:
            leases_server = self._bots[name].leases

        # if this a zombie bot reporting to its old name then error.
        if self._bots[name].bot_id != bot_session.bot_id:
            raise InvalidArgumentError("Bot with name={} was not found with this id".format(name))

        # Generate a list of all the bots that are reporting with this id but
        # not this name. Per the spec, any bot that is reporting an ID that
        # does not match the name we have file for them should not be given any
        # work.
        for _name, _bot in list(self._bots.items()):
            if _bot.bot_id == bot_session.bot_id and _name != name:
                self.logger.warn("Duplicate bot_id provided of {}: this is registered under names {} and {}. Closing session with {}."
                                 .format(bot_session.bot_id, name, _name, _name))
                self._close_bot_session(_name)

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

    def enqueue_action(self, operation_name, action, priority = 10):
        item = namedtuple('ActionQueue', 'operation_name action')
        action_any = any_pb2.Any()
        action_any.Pack(action)
        self._action_queue.put((priority, item(operation_name, action_any)))

    def enqueue_operation(self, operation_name, stage):
        item = namedtuple('OperationQueue', 'operation_name stage')
        self.operation_queue.put(item(operation_name, stage))

    def _check_lease(self, lease):
        """ Assigns work to available leases. Any completed leases should notify
        the Operations Service by queuing the operation name along with the status.
        """
        state = lease.state
        state_enum = bots_pb2.LeaseState

        if state   == state_enum.Value('LEASE_STATE_UNSPECIFIED'):
            return self._get_pending_action(lease)

        elif state == state_enum.Value('PENDING'):
            # Pottentially raise a warning that lease
            # hasn't been accepted?
            return lease

        elif state == state_enum.Value('ACTIVE'):
            return lease

        elif state == state_enum.Value('COMPLETED'):
            self.logger.debug("Got completed work.")
            operation_name = lease.assignment
            self.enqueue_operation(operation_name, 'COMPLETED')
            return self._get_pending_action(lease)

        elif state == state_enum.Value('CANCELLED'):
            raise NotImplementedError

        else:
            raise InvalidArgumentError("Unknown state: {}".format(state))

    def _get_pending_action(self, lease):
        """ If actions are available, populates the lease and
        informats the execution service, else it returns an
        empty lease.
        """
        if not self._action_queue.empty():
            operation_name, action = self._action_queue.get()[1]
            self.enqueue_operation(operation_name, 'EXECUTING')
            lease = bots_pb2.Lease(assignment = operation_name,
                                   inline_assignment = action,
                                   state = bots_pb2.LeaseState.Value('PENDING'))
            return lease
        return bots_pb2.Lease()

    def _requeue_lease_if_applicable(self, lease):
        state = lease.state
        state_enum = bots_pb2.LeaseState
        if state == state_enum.Value('PENDING') or \
           state == state_enum.Value('ACTIVE'):
            item = namedtuple('ActionQueue', 'operation_name action')
            operation_name = lease.assignment
            action = lease.inline_assignment
            self._action_queue.put((1, item(operation_name, action)))

    def _close_bot_session(self, name):
        """ Before removing the session, close any leases and
        requeue with high priority.
        """
        try:
            bot = self._bots[name]
        except KeyError:
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
