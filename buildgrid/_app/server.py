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
BuildGridServer
==============

Creates a BuildGrid server, binding all the requisite service instances together.
"""

import logging
from concurrent import futures

import grpc

from buildgrid.server.cas.service import ByteStreamService, ContentAddressableStorageService
from buildgrid.server.actioncache.service import ActionCacheService
from buildgrid.server.execution.service import ExecutionService
from buildgrid.server.operations.service import OperationsService
from buildgrid.server.bots.service import BotsService
from buildgrid.server.referencestorage.service import ReferenceStorageService


class BuildGridServer:

    def __init__(self, port=50051, max_workers=10, credentials=None,
                 execution_instances=None, bots_interfaces=None, operations_instances=None,
                 operations_service_instances=None, reference_storage_instances=None,
                 action_cache_instances=None, cas_instances=None, bytestream_instances=None):

        self.logger = logging.getLogger(__name__)
        address = '[::]:{0}'.format(port)

        server = grpc.server(futures.ThreadPoolExecutor(max_workers))

        if credentials is not None:
            self.logger.info("Secure connection")
            server.add_secure_port(address, credentials)

        else:
            self.logger.info("Insecure connection")
            server.add_insecure_port(address)

        if execution_instances:
            self.logger.debug("Adding execution instances {}".format(
                execution_instances.keys()))
            ExecutionService(server, execution_instances)

        if bots_interfaces:
            self.logger.debug("Adding bots interfaces {}".format(
                bots_interfaces.keys()))
            BotsService(server, bots_interfaces)

        if operations_instances:
            self.logger.debug("Adding operations instances {}".format(
                operations_instances.keys()))
            OperationsService(server, operations_instances)

        if reference_storage_instances:
            self.logger.debug("Adding reference storages {}".format(
                reference_storage_instances.keys()))
            ReferenceStorageService(server, reference_storage_instances)

        if action_cache_instances:
            self.logger.debug("Adding action cache instances {}".format(
                action_cache_instances.keys()))
            ActionCacheService(server, action_cache_instances)

        if cas_instances:
            self.logger.debug("Adding cas instances {}".format(
                cas_instances.keys()))
            ContentAddressableStorageService(server, cas_instances)

        if bytestream_instances:
            self.logger.debug("Adding bytestream instances {}".format(
                bytestream_instances.keys()))
            ByteStreamService(server, bytestream_instances)

        self._server = server

    def start(self):
        self._server.start()

    def stop(self):
        self._server.stop(grace=0)
