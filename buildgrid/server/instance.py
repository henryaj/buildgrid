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


import logging
import os
from concurrent import futures

import grpc

from .cas.service import ByteStreamService, ContentAddressableStorageService
from .actioncache.service import ActionCacheService
from .execution.service import ExecutionService
from .operations.service import OperationsService
from .bots.service import BotsService
from .referencestorage.service import ReferenceStorageService


class BuildGridServer:
    """Creates a BuildGrid server.

    The :class:`BuildGridServer` class binds together all the
    requisite services.
    """

    def __init__(self, max_workers=None):
        """Initializes a new :class:`BuildGridServer` instance.

        Args:
            max_workers (int, optional): A pool of max worker threads.
        """

        self.logger = logging.getLogger(__name__)

        if max_workers is None:
            # Use max_workers default from Python 3.5+
            max_workers = (os.cpu_count() or 1) * 5

        server = grpc.server(futures.ThreadPoolExecutor(max_workers))

        self._server = server

        self._execution_service = None
        self._bots_service = None
        self._operations_service = None
        self._reference_storage_service = None
        self._action_cache_service = None
        self._cas_service = None
        self._bytestream_service = None

    def start(self):
        """Starts the server.
        """
        self._server.start()

    def stop(self, grace=0):
        """Stops the server.
        """
        self._server.stop(grace)

    def add_port(self, address, credentials):
        """Adds a port to the server.

        Must be called before the server starts. If a credentials object exists,
        it will make a secure port.

        Args:
            address (str): The address with port number.
            credentials (:obj:`grpc.ChannelCredentials`): Credentials object.
        """
        if credentials is not None:
            self.logger.info("Adding secure connection on: [{}]".format(address))
            self._server.add_secure_port(address, credentials)

        else:
            self.logger.info("Adding insecure connection on [{}]".format(address))
            self._server.add_insecure_port(address)

    def add_execution_instance(self, instance, instance_name):
        """Adds an :obj:`ExecutionInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ExecutionInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._execution_service is None:
            self._execution_service = ExecutionService(self._server)

        self._execution_service.add_instance(instance_name, instance)

    def add_bots_interface(self, instance, instance_name):
        """Adds a :obj:`BotsInterface` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`BotsInterface`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._bots_service is None:
            self._bots_service = BotsService(self._server)

        self._bots_service.add_instance(instance_name, instance)

    def add_operations_instance(self, instance, instance_name):
        """Adds an :obj:`OperationsInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`OperationsInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._operations_service is None:
            self._operations_service = OperationsService(self._server)

        self._operations_service.add_instance(instance_name, instance)

    def add_reference_storage_instance(self, instance, instance_name):
        """Adds a :obj:`ReferenceCache` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._reference_storage_service is None:
            self._reference_storage_service = ReferenceStorageService(self._server)

        self._reference_storage_service.add_instance(instance_name, instance)

    def add_action_cache_instance(self, instance, instance_name):
        """Adds a :obj:`ReferenceCache` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._action_cache_service is None:
            self._action_cache_service = ActionCacheService(self._server)

        self._action_cache_service.add_instance(instance_name, instance)

    def add_cas_instance(self, instance, instance_name):
        """Stores a :obj:`ContentAddressableStorageInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._cas_service is None:
            self._cas_service = ContentAddressableStorageService(self._server)

        self._cas_service.add_instance(instance_name, instance)

    def add_bytestream_instance(self, instance, instance_name):
        """Stores a :obj:`ByteStreamInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ByteStreamInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._bytestream_service is None:
            self._bytestream_service = ByteStreamService(self._server)

        self._bytestream_service.add_instance(instance_name, instance)
