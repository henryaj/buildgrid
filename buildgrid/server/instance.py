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


import asyncio
from concurrent import futures
import logging
import os

import grpc

from buildgrid.server.actioncache.service import ActionCacheService
from buildgrid.server.bots.service import BotsService
from buildgrid.server.cas.service import ByteStreamService, ContentAddressableStorageService
from buildgrid.server.execution.service import ExecutionService
from buildgrid.server._monitoring import MonitoringBus, MonitoringOutputType, MonitoringOutputFormat
from buildgrid.server.operations.service import OperationsService
from buildgrid.server.referencestorage.service import ReferenceStorageService


class BuildGridServer:
    """Creates a BuildGrid server.

    The :class:`BuildGridServer` class binds together all the
    requisite services.
    """

    def __init__(self, max_workers=None, monitor=False):
        """Initializes a new :class:`BuildGridServer` instance.

        Args:
            max_workers (int, optional): A pool of max worker threads.
        """
        self.__logger = logging.getLogger(__name__)

        if max_workers is None:
            # Use max_workers default from Python 3.5+
            max_workers = (os.cpu_count() or 1) * 5

        self.__grpc_executor = futures.ThreadPoolExecutor(max_workers)
        self.__grpc_server = grpc.server(self.__grpc_executor)

        self.__main_loop = asyncio.get_event_loop()
        self.__monitoring_bus = None

        self._execution_service = None
        self._bots_service = None
        self._operations_service = None
        self._reference_storage_service = None
        self._action_cache_service = None
        self._cas_service = None
        self._bytestream_service = None

        self._is_instrumented = monitor

        if self._is_instrumented:
            self.__monitoring_bus = MonitoringBus(
                self.__main_loop, endpoint_type=MonitoringOutputType.STDOUT,
                serialisation_format=MonitoringOutputFormat.JSON)

    # --- Public API ---

    def start(self):
        """Starts the BuildGrid server."""
        self.__grpc_server.start()

        if self._is_instrumented:
            self.__monitoring_bus.start()

        self.__main_loop.run_forever()

    def stop(self):
        """Stops the BuildGrid server."""
        if self._is_instrumented:
            self.__monitoring_bus.stop()

        self.__main_loop.stop()

        self.__grpc_server.stop(None)

    def add_port(self, address, credentials):
        """Adds a port to the server.

        Must be called before the server starts. If a credentials object exists,
        it will make a secure port.

        Args:
            address (str): The address with port number.
            credentials (:obj:`grpc.ChannelCredentials`): Credentials object.
        """
        if credentials is not None:
            self.__logger.info("Adding secure connection on: [%s]", address)
            self.__grpc_server.add_secure_port(address, credentials)

        else:
            self.__logger.info("Adding insecure connection on [%s]", address)
            self.__grpc_server.add_insecure_port(address)

    def add_execution_instance(self, instance, instance_name):
        """Adds an :obj:`ExecutionInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ExecutionInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._execution_service is None:
            self._execution_service = ExecutionService(self.__grpc_server)

        self._execution_service.add_instance(instance_name, instance)

    def add_bots_interface(self, instance, instance_name):
        """Adds a :obj:`BotsInterface` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`BotsInterface`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._bots_service is None:
            self._bots_service = BotsService(self.__grpc_server)

        self._bots_service.add_instance(instance_name, instance)

    def add_operations_instance(self, instance, instance_name):
        """Adds an :obj:`OperationsInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`OperationsInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._operations_service is None:
            self._operations_service = OperationsService(self.__grpc_server)

        self._operations_service.add_instance(instance_name, instance)

    def add_reference_storage_instance(self, instance, instance_name):
        """Adds a :obj:`ReferenceCache` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._reference_storage_service is None:
            self._reference_storage_service = ReferenceStorageService(self.__grpc_server)

        self._reference_storage_service.add_instance(instance_name, instance)

    def add_action_cache_instance(self, instance, instance_name):
        """Adds a :obj:`ReferenceCache` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._action_cache_service is None:
            self._action_cache_service = ActionCacheService(self.__grpc_server)

        self._action_cache_service.add_instance(instance_name, instance)

    def add_cas_instance(self, instance, instance_name):
        """Stores a :obj:`ContentAddressableStorageInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ReferenceCache`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._cas_service is None:
            self._cas_service = ContentAddressableStorageService(self.__grpc_server)

        self._cas_service.add_instance(instance_name, instance)

    def add_bytestream_instance(self, instance, instance_name):
        """Stores a :obj:`ByteStreamInstance` to the service.

        If no service exists, it creates one.

        Args:
            instance (:obj:`ByteStreamInstance`): Instance to add.
            instance_name (str): Instance name.
        """
        if self._bytestream_service is None:
            self._bytestream_service = ByteStreamService(self.__grpc_server)

        self._bytestream_service.add_instance(instance_name, instance)

    # --- Public API: Monitoring ---

    @property
    def is_instrumented(self):
        return self._is_instrumented
