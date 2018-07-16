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
Bot
====

Creates a bot session.
"""

import asyncio
import inspect
import logging
import platform
import queue
import time

from google.devtools.remoteworkers.v1test2 import bots_pb2, worker_pb2

from . import bot_interface
from .._exceptions import BotError

class Bot(object):
    """
    Creates a local BotSession.
    """

    def __init__(self, work, context, channel, parent, number_of_leases):
        if not inspect.iscoroutinefunction(work):
            raise BotError("work function must be async")

        self.interface = bot_interface.BotInterface(channel)
        self.logger = logging.getLogger(__name__)

        self._create_session(parent, number_of_leases)
        self._work_queue = queue.Queue(maxsize = number_of_leases)

        try:
            while True:
                ## TODO: Leases independently finish
                ## Allow leases to queue finished work independently instead
                ## of waiting for all to finish
                futures = [self._do_work(work, context, lease) for lease in self._get_work()]
                if futures:
                    loop = asyncio.new_event_loop()
                    leases_complete, _ = loop.run_until_complete(asyncio.wait(futures))
                    work_complete = [(lease.result().assignment, lease.result(),) for lease in leases_complete]
                    self._work_complete(work_complete)
                    loop.close()
                self._update_bot_session()
                time.sleep(2)
        except Exception as e:
            self.logger.error(e)
            raise BotError(e)

    @property
    def bot_session(self):
        ## Read only, shouldn't have to set any of the variables in here
        return self._bot_session

    def close_session(self):
        self.logger.warning("Session closing not yet implemented")

    async def _do_work(self, work, context, lease):
        """ Work is done here, work function should be asynchronous
        """
        self.logger.info("Work found: {}".format(lease.assignment))
        lease = await work(context=context, lease=lease)
        lease.state = bots_pb2.LeaseState.Value('COMPLETED')
        self.logger.info("Work complete: {}".format(lease.assignment))
        return lease

    def _update_bot_session(self):
        """ Should call the server periodically to inform the server the client
        has not died.
        """
        self.logger.debug("Updating bot session")
        self._bot_session = self.interface.update_bot_session(self._bot_session)
        leases_update = ([self._update_lease(lease) for lease in self._bot_session.leases])
        del self._bot_session.leases[:]
        self._bot_session.leases.extend(leases_update)

    def _get_work(self):
        while not self._work_queue.empty():
            yield self._work_queue.get()
            self._work_queue.task_done()

    def _work_complete(self, leases_complete):
        """ Bot updates itself with any completed work.
        """
        # Should really improve this...
        # Maybe add some call back function sentoff work...
        leases_active = list(filter(self._lease_active, self._bot_session.leases))
        leases_not_active = [lease for lease in self._bot_session.leases if not self._lease_active(lease)]
        del self._bot_session.leases[:]
        for lease in leases_active:
            for lease_tuple in leases_complete:
                if lease.assignment == lease_tuple[0]:
                    leases_not_active.extend([lease_tuple[1]])
        self._bot_session.leases.extend(leases_not_active)

    def _update_lease(self, lease):
        """
        State machine for any recieved updates to the leases.
        """
        if self._lease_pending(lease):
            lease.state = bots_pb2.LeaseState.Value('ACTIVE')
            self._work_queue.put(lease)
            return lease

        else:
            return lease

    def _create_session(self, parent, number_of_leases):
        self.logger.debug("Creating bot session")
        worker = self._create_worker()

        """ Unique bot ID within the farm used to identify this bot
        Needs to be human readable.
        All prior sessions with bot_id of same ID are invalidated.
        If a bot attempts to update an invalid session, it must be rejected and
        may be put in quarantine.
        """
        bot_id = '{}.{}'.format(parent, platform.node())
        
        leases = [bots_pb2.Lease() for x in range(number_of_leases)]
        
        bot_session = bots_pb2.BotSession(worker = worker,
                                   status = bots_pb2.BotStatus.Value('OK'),
                                   leases = leases,
                                   bot_id = bot_id)
        self._bot_session = self.interface.create_bot_session(parent, bot_session)
        self.logger.info("Name: {}, Id: {}".format(self._bot_session.name,
                                                      self._bot_session.bot_id))

    def _create_worker(self):
        devices = self._create_devices()

        # Contains a list of devices and the connections between them.
        worker = worker_pb2.Worker(devices = devices)

        """ Keys supported:
        *pool
        """
        worker.Property.key = "pool"
        worker.Property.value = "all"

        return worker

    def _create_devices(self):
        """ Creates devices available to the worker
        The first device is know as the Primary Device - the revice which
        is running a bit and responsible to actually executing commands.
        All other devices are known as Attatched Devices and must be controlled
        by the Primary Device.
        """

        devices = []

        for i in range(0, 1): # Append one device for now
            dev = worker_pb2.Device()

            devices.append(dev)

        return devices

    def _lease_pending(self, lease):
        return lease.state == bots_pb2.LeaseState.Value('PENDING')

    def _lease_active(self, lease):
        return lease.state == bots_pb2.LeaseState.Value('ACTIVE')
