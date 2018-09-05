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
ExecutionService
===============

Serves remote execution requests.
"""

import logging
import queue
from functools import partial

import grpc

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc

from buildgrid._protos.google.longrunning import operations_pb2

from .._exceptions import InvalidArgumentError


class ExecutionService(remote_execution_pb2_grpc.ExecutionServicer):

    def __init__(self, server, instances):
        self.logger = logging.getLogger(__name__)
        self._instances = instances

        remote_execution_pb2_grpc.add_ExecutionServicer_to_server(self, server)

    def Execute(self, request, context):
        try:
            message_queue = queue.Queue()
            instance = self._get_instance(request.instance_name)
            operation = instance.execute(request.action_digest,
                                         request.skip_cache_lookup,
                                         message_queue)

            context.add_callback(partial(instance.unregister_message_client,
                                         operation.name, message_queue))

            yield from instance.stream_operation_updates(message_queue,
                                                         operation.name)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield operations_pb2.Operation()

    def WaitExecution(self, request, context):
        try:
            names = request.name.split("/")

            # Operation name should be in format:
            # {instance/name}/{operation_id}
            instance_name = ''.join(names[0:-1])

            message_queue = queue.Queue()
            operation_name = names[-1]
            instance = self._get_instance(instance_name)

            instance.register_message_client(operation_name, message_queue)

            context.add_callback(partial(instance.unregister_message_client,
                                         operation_name, message_queue))

            yield from instance.stream_operation_updates(message_queue,
                                                         operation_name)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield operations_pb2.Operation()

    def _get_instance(self, name):
        try:
            return self._instances[name]

        except KeyError:
            raise InvalidArgumentError("Instance doesn't exist on server: {}".format(name))