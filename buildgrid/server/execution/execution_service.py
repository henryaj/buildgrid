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
ExecutionService
===============

Serves remote execution requests.
"""

import grpc
import logging

from google.devtools.remoteexecution.v1test import remote_execution_pb2, remote_execution_pb2_grpc
from google.longrunning import operations_pb2_grpc, operations_pb2

from ._exceptions import InvalidArgumentError

class ExecutionService(remote_execution_pb2_grpc.ExecutionServicer):

    def __init__(self, instance):
        self._instance = instance
        self.logger = logging.getLogger(__name__)

    def Execute(self, request, context):
        # Ignore request.instance_name for now
        # Have only one instance
        try:
            return self._instance.execute(request.action,
                                          request.skip_cache_lookup)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return operations_pb2.Operation()

        except NotImplementedError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            return operations_pb2.Operation()
