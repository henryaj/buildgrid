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
BuildGridServer
==============

Creates the user a local server BuildGrid server.
"""

import time
import grpc
from concurrent import futures

from google.devtools.remoteexecution.v1test import remote_execution_pb2_grpc
from google.devtools.remoteworkers.v1test2 import bots_pb2_grpc
from google.longrunning import operations_pb2_grpc

from .execution.execution_service import ExecutionService
from .execution.operations_service import OperationsService
from .execution.execution_instance import ExecutionInstance
from .scheduler import Scheduler
from .worker.bots_service import BotsService
from .worker.bots_interface import BotsInterface

class BuildGridServer(object):

    def __init__(self, port = '50051', max_workers = 10):
        port = '[::]:{0}'.format(port)
        scheduler = Scheduler()
        bots_interface = BotsInterface(scheduler)
        execution_instance = ExecutionInstance(scheduler)

        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers))
        self._server.add_insecure_port(port)

        bots_pb2_grpc.add_BotsServicer_to_server(BotsService(bots_interface),
                                                 self._server)
        remote_execution_pb2_grpc.add_ExecutionServicer_to_server(ExecutionService(execution_instance),
                                                                  self._server)
        operations_pb2_grpc.add_OperationsServicer_to_server(OperationsService(execution_instance),
                                                             self._server)

    async def start(self):
        self._server.start()

    async def stop(self):
        self._server.stop(0)
