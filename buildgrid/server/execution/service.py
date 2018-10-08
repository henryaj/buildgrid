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

from buildgrid._exceptions import FailedPreconditionError, InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc
from buildgrid._protos.google.longrunning import operations_pb2


class ExecutionService(remote_execution_pb2_grpc.ExecutionServicer):

    def __init__(self, server):
        self.logger = logging.getLogger(__name__)
        self._instances = {}
        remote_execution_pb2_grpc.add_ExecutionServicer_to_server(self, server)

    def add_instance(self, name, instance):
        self._instances[name] = instance

    def Execute(self, request, context):
        try:
            self.logger.debug("Execute request from [{}]"
                              .format(context.peer()))
            message_queue = queue.Queue()
            instance = self._get_instance(request.instance_name)
            operation = instance.execute(request.action_digest,
                                         request.skip_cache_lookup,
                                         message_queue)

            context.add_callback(partial(instance.unregister_message_client,
                                         operation.name, message_queue))

            instanced_op_name = "{}/{}".format(request.instance_name,
                                               operation.name)

            self.logger.info("Operation name: [{}]".format(instanced_op_name))

            for operation in instance.stream_operation_updates(message_queue,
                                                               operation.name):
                op = operations_pb2.Operation()
                op.CopyFrom(operation)
                op.name = instanced_op_name
                yield op

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield operations_pb2.Operation()

        except FailedPreconditionError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            yield operations_pb2.Operation()

    def WaitExecution(self, request, context):
        try:
            self.logger.debug("WaitExecution request from [{}]"
                              .format(context.peer()))
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

            for operation in instance.stream_operation_updates(message_queue,
                                                               operation_name):
                op = operations_pb2.Operation()
                op.CopyFrom(operation)
                op.name = request.name
                yield op

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield operations_pb2.Operation()

    def _get_instance(self, name):
        try:
            return self._instances[name]

        except KeyError:
            raise InvalidArgumentError("Instance doesn't exist on server: [{}]".format(name))
