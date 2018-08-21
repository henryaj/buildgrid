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

from .cas.bytestream_service import ByteStreamService
from .cas.content_addressable_storage_service import ContentAddressableStorageService
from .execution.action_cache_service import ActionCacheService
from .execution.execution_service import ExecutionService
from .execution.operations_service import OperationsService
from .execution.execution_instance import ExecutionInstance
from .scheduler import Scheduler
from .worker.bots_service import BotsService
from .worker.bots_interface import BotsInterface


class BuildGridServer:

    def __init__(self, port='50051', max_workers=10, cas_storage=None, action_cache=None):
        port = '[::]:{0}'.format(port)
        scheduler = Scheduler(action_cache)
        bots_interface = BotsInterface(scheduler)
        execution_instance = ExecutionInstance(scheduler, cas_storage)

        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers))
        self._server.add_insecure_port(port)

        bots_pb2_grpc.add_BotsServicer_to_server(BotsService(bots_interface),
                                                 self._server)
        remote_execution_pb2_grpc.add_ExecutionServicer_to_server(ExecutionService(execution_instance),
                                                                  self._server)
        operations_pb2_grpc.add_OperationsServicer_to_server(OperationsService(execution_instance),
                                                             self._server)

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

    def start(self):
        self._server.start()

    def stop(self):
        self._server.stop(0)
