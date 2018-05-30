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
Bot command
=================

Create a bot interface and request work
"""

import asyncio
import click
import grpc
import platform
import random
import time
import queue

from ...server import build_grid_server
from ..cli import pass_context

from google.devtools.remoteworkers.v1test2 import bots_pb2, bots_pb2_grpc, worker_pb2

@click.group(short_help = 'Simple bot client')
@click.option('--port', default='50051')
@pass_context
def cli(context, port):
    context.vlog("Verbose mode!")
    context.log("Starting on port {}".format(port))

    context.channel = grpc.insecure_channel('localhost:{}'.format(port))
    context.port = port

@cli.command('create', short_help='Create a bot session')
@click.argument('parent', default='bgd_test')
@click.option('--continuous', is_flag=True)
@pass_context
def create(context, parent, continuous):
    """
    Simple dummy client. Creates a session, accepts leases, does work and
    updates the server. Can run this in continious mode.
    """

    context.log("Creating a bot session...\n")

    bot_interface = BotInterface(context)
    bot = Bot(parent, bot_interface)

    context.log("Bot id: {}".format(bot.bot_session.bot_id))
    bot.update_bot_session()
    context.log("Name:   {}".format(bot.bot_session.name))
    
    try:
        while True:
            futures = [_do_work(context, lease) for lease in bot.get_work()]
            if futures:
                context.log("Work found...")
                loop = asyncio.get_event_loop()
                leases_complete, _ = loop.run_until_complete(asyncio.wait(futures))
                work_complete = [(lease.result().assignment, lease.result(),) for lease in leases_complete]
                bot.work_complete(work_complete)
                loop.close()
            bot.update_bot_session()
            if not continuous: break
            time.sleep(2)
            
    except KeyboardInterrupt:
        return

async def _do_work(context, lease):
    await asyncio.sleep(random.randint(1,5))
    context.log("Work complete")
    lease.state = bots_pb2.LeaseState.Value('COMPLETED')
    return lease

class BotInterface(object):
    """ Interface handles calls to the server.
    """

    def __init__(self, context):
        self.context = context
        self._stub = bots_pb2_grpc.BotsStub(context.channel)

    def create_bot_session(self, bot_session, parent):
        self.context.log("Creating bot session")
        request = bots_pb2.CreateBotSessionRequest(parent = parent,
                                                   bot_session = bot_session)
        return self._stub.CreateBotSession(request)

    def update_bot_session(self, bot_session):
        self.context.log("Updating server")
        request = bots_pb2.UpdateBotSessionRequest(name = bot_session.name,
                                                   bot_session = bot_session,
                                                   update_mask = None) ## TODO: add mask
        return self._stub.UpdateBotSession(request)

class Bot(object):
    """
    Creates a local BotSession.
    """
    NUMBER_OF_LEASES = 3

    def __init__(self, parent, interface):
        self.bot_session = interface.create_bot_session(self._create_session(parent), parent)
        self._work_queue = queue.Queue(maxsize = self.NUMBER_OF_LEASES)
        self.interface = interface

    def update_bot_session(self):
        """ Should be call the server periodically to inform the server the client
        has not died.
        """
        self.bot_session = self.interface.update_bot_session(self.bot_session)
        leases_update = ([self._update_lease(lease) for lease in self.bot_session.leases])
        del self.bot_session.leases[:]
        self.bot_session.leases.extend(leases_update)

    def get_work(self):
        while not self._work_queue.empty():
            yield self._work_queue.get()
            self._work_queue.task_done()

    def work_complete(self, leases_complete):
        """ Bot updates itself with any completed work.
        """
        # Should really improve this...
        # Maybe add some call back function sentoff work...
        leases_active = list(filter(self._lease_active, self.bot_session.leases))
        leases_not_active = [lease for lease in self.bot_session.leases if not self._lease_active(lease)]
        del self.bot_session.leases[:]
        for lease in leases_active:
            for lease_tuple in leases_complete:
                if lease.assignment == lease_tuple[0]:
                    leases_not_active.extend([lease_tuple[1]])
        self.bot_session.leases.extend(leases_not_active)

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

    def _create_session(self, parent):
        worker = self._create_worker()
    
        """ Unique bot ID within the farm used to identify this bot
        Needs to be human readable.
        All prior sessions with bot_id of same ID are invalidated.
        If a bot attempts to update an invalid session, it must be rejected and
        may be put in quarantine.
        """
        bot_id = '{}.{}'.format(parent, platform.node())
        
        leases = [bots_pb2.Lease() for x in range(self.NUMBER_OF_LEASES)]
        
        bot_session = bots_pb2.BotSession(worker = worker,
                                   status = bots_pb2.BotStatus.Value('OK'),
                                   leases = leases,
                                   bot_id = bot_id)
        return bot_session

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
