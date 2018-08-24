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

Creates the user a local server BuildGrid server.
"""

from concurrent import futures

import grpc

from buildgrid._protos.google.bytestream import bytestream_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2_grpc
from buildgrid._protos.google.longrunning import operations_pb2_grpc

from .buildgrid_instance import BuildGridInstance
from .cas.bytestream_service import ByteStreamService
from .cas.content_addressable_storage_service import ContentAddressableStorageService
from .execution.action_cache_service import ActionCacheService
from .execution.execution_service import ExecutionService
from .execution.operations_service import OperationsService
from .worker.bots_service import BotsService


class BuildGridServer:

    def __init__(self, port=50051, credentials=None, instances=None,
                 max_workers=10, action_cache=None, cas_storage=None):
        address = '[::]:{0}'.format(port)

        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers))

        if credentials is not None:
            self._server.add_secure_port(address, credentials)
        else:
            self._server.add_insecure_port(address)

        if cas_storage is not None:
            cas_service = ContentAddressableStorageService(cas_storage)
            remote_execution_pb2_grpc.add_ContentAddressableStorageServicer_to_server(cas_service,
                                                                                      self._server)
            bytestream_pb2_grpc.add_ByteStreamServicer_to_server(ByteStreamService(cas_storage),
                                                                 self._server)
        if action_cache is not None:
            action_cache_service = ActionCacheService(action_cache)
            remote_execution_pb2_grpc.add_ActionCacheServicer_to_server(action_cache_service,
                                                                        self._server)

        buildgrid_instances = {}
        if not instances:
            buildgrid_instances["main"] = BuildGridInstance(action_cache, cas_storage)
        else:
            for name in instances:
                buildgrid_instances[name] = BuildGridInstance(action_cache, cas_storage)

        bots_pb2_grpc.add_BotsServicer_to_server(BotsService(buildgrid_instances),
                                                 self._server)
        remote_execution_pb2_grpc.add_ExecutionServicer_to_server(ExecutionService(buildgrid_instances),
                                                                  self._server)
        operations_pb2_grpc.add_OperationsServicer_to_server(OperationsService(buildgrid_instances),
                                                             self._server)

    def start(self):
        self._server.start()

    def stop(self):
        self._server.stop(0)
