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
====

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
    def __init__(self, parent, interface):
        """ Unique bot ID within the farm used to identify this bot
        Needs to be human readable.
        All prior sessions with bot_id of same ID are invalidated.
        If a bot attempts to update an invalid session, it must be rejected and
        may be put in quarantine.
        """

        self.logger = logging.getLogger(__name__)

        self._bot_id = '{}.{}'.format(parent, platform.node())
        self._context = None
        self._interface = interface
        self._leases = {}
        self._name = None
        self._parent = parent
        self._status = BotStatus.OK.value
        self._work = None
        self._worker = None

    @property
    def bot_id(self):
        return self._bot_id

    def add_worker(self, worker):
        self._worker = worker

    def create_bot_session(self, work, context=None):
        self.logger.debug("Creating bot session")
        self._work = work
        self._context = context

        session = self._interface.create_bot_session(self._parent, self.get_pb2())
        self._name = session.name

        self.logger.info("Created bot session with name: [{}]".format(self._name))

        for lease in session.leases:
            self._update_lease_from_server(lease)

    def update_bot_session(self):
        self.logger.debug("Updating bot session: [{}]".format(self._bot_id))
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
                                   bot_id=self._bot_id,
                                   name=self._name)

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
        self.logger.debug("Work created: [{}]".format(lease.id))
        loop = asyncio.get_event_loop()

        try:
            lease = await loop.run_in_executor(None, self._work, self._context, lease)

        except grpc.RpcError as e:
            self.logger.error("RPC error thrown: [{}]".format(e))
            lease.status.CopyFrom(e.code())

        except BotError as e:
            self.logger.error("Internal bot error thrown: [{}]".format(e))
            lease.status.code = code_pb2.INTERNAL

        except Exception as e:
            self.logger.error("Exception thrown: [{}]".format(e))
            lease.status.code = code_pb2.INTERNAL

        self.logger.debug("Work complete: [{}]".format(lease.id))
        self.lease_completed(lease)


class Worker:
    def __init__(self, properties=None, configs=None):
        self.properties = {}
        self._configs = {}
        self._devices = []

        if properties:
            for k, v in properties.items():
                if k == 'pool':
                    self.properties[k] = v
                else:
                    raise KeyError('Key not supported: [{}]'.format(k))

        if configs:
            for k, v in configs.items():
                if k == 'DockerImage':
                    self.configs[k] = v
                else:
                    raise KeyError('Key not supported: [{}]'.format(k))

    @property
    def configs(self):
        return self._configs

    def add_device(self, device):
        self._devices.append(device)

    def get_pb2(self):
        devices = [device.get_pb2() for device in self._devices]
        worker = worker_pb2.Worker(devices=devices)
        property_message = worker_pb2.Worker.Property()
        for k, v in self.properties.items():
            property_message.key = k
            property_message.value = v
            worker.properties.extend([property_message])

        config_message = worker_pb2.Worker.Config()
        for k, v in self.properties.items():
            property_message.key = k
            property_message.value = v
            worker.configs.extend([config_message])

        return worker


class Device:
    def __init__(self, properties=None):
        """ Creates devices available to the worker
        The first device is know as the Primary Device - the revice which
        is running a bit and responsible to actually executing commands.
        All other devices are known as Attatched Devices and must be controlled
        by the Primary Device.
        """

        self._name = str(uuid.uuid4())
        self._properties = {}

        if properties:
            for k, v in properties.items():
                if k == 'os':
                    self._properties[k] = v

                elif k == 'docker':
                    if v not in ('True', 'False'):
                        raise ValueError('Value not supported: [{}]'.format(v))
                    self._properties[k] = v

                else:
                    raise KeyError('Key not supported: [{}]'.format(k))

    @property
    def name(self):
        return self._name

    @property
    def properties(self):
        return self._properties

    def get_pb2(self):
        device = worker_pb2.Device(handle=self._name)
        property_message = worker_pb2.Device.Property()
        for k, v in self._properties.items():
            property_message.key = k
            property_message.value = v
            device.properties.extend([property_message])
        return device
