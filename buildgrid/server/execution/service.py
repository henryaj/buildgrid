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

from buildgrid._exceptions import FailedPreconditionError, InvalidArgumentError, CancelledError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc
from buildgrid._protos.google.longrunning import operations_pb2


class ExecutionService(remote_execution_pb2_grpc.ExecutionServicer):

    def __init__(self, server, monitor=False):
        self.__logger = logging.getLogger(__name__)

        self.__peers_by_instance = None
        self.__peers = None

        self._instances = {}

        remote_execution_pb2_grpc.add_ExecutionServicer_to_server(self, server)

        self._is_instrumented = monitor

        if self._is_instrumented:
            self.__peers_by_instance = {}
            self.__peers = {}

    # --- Public API ---

    def add_instance(self, instance_name, instance):
        self._instances[instance_name] = instance

        if self._is_instrumented:
            self.__peers_by_instance[instance_name] = set()

    # --- Public API: Servicer ---

    def Execute(self, request, context):
        """Handles ExecuteRequest messages.

        Args:
            request (ExecuteRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.debug("Execute request from [%s]", context.peer())

        instance_name = request.instance_name
        message_queue = queue.Queue()
        peer = context.peer()

        try:
            instance = self._get_instance(instance_name)
            operation = instance.execute(request.action_digest,
                                         request.skip_cache_lookup,
                                         message_queue)

            context.add_callback(partial(self._rpc_termination_callback,
                                         peer, instance_name, operation.name, message_queue))

            if self._is_instrumented:
                if peer not in self.__peers:
                    self.__peers_by_instance[instance_name].add(peer)
                    self.__peers[peer] = 1
                else:
                    self.__peers[peer] += 1

            instanced_op_name = "{}/{}".format(instance_name, operation.name)

            self.__logger.info("Operation name: [%s]", instanced_op_name)

            for operation in instance.stream_operation_updates(message_queue,
                                                               operation.name):
                op = operations_pb2.Operation()
                op.CopyFrom(operation)
                op.name = instanced_op_name
                yield op

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield operations_pb2.Operation()

        except FailedPreconditionError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            yield operations_pb2.Operation()

        except CancelledError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.CANCELLED)
            yield operations_pb2.Operation()

    def WaitExecution(self, request, context):
        """Handles WaitExecutionRequest messages.

        Args:
            request (WaitExecutionRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.debug("WaitExecution request from [%s]", context.peer())

        names = request.name.split('/')
        instance_name = '/'.join(names[:-1])
        operation_name = names[-1]
        message_queue = queue.Queue()
        peer = context.peer()

        try:
            instance = self._get_instance(instance_name)

            instance.register_message_client(operation_name, message_queue)
            context.add_callback(partial(self._rpc_termination_callback,
                                         peer, instance_name, operation_name, message_queue))

            if self._is_instrumented:
                if peer not in self.__peers:
                    self.__peers_by_instance[instance_name].add(peer)
                    self.__peers[peer] = 1
                else:
                    self.__peers[peer] += 1

            for operation in instance.stream_operation_updates(message_queue,
                                                               operation_name):
                op = operations_pb2.Operation()
                op.CopyFrom(operation)
                op.name = request.name
                yield op

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield operations_pb2.Operation()

        except CancelledError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.CANCELLED)
            yield operations_pb2.Operation()

    # --- Public API: Monitoring ---

    @property
    def is_instrumented(self):
        return self._is_instrumented

    def query_n_clients(self):
        if self.__peers is not None:
            return len(self.__peers)
        return 0

    def query_n_clients_for_instance(self, instance_name):
        try:
            if self.__peers_by_instance is not None:
                return len(self.__peers_by_instance[instance_name])
        except KeyError:
            pass
        return 0

    # --- Private API ---

    def _rpc_termination_callback(self, peer, instance_name, job_name, message_queue):
        instance = self._get_instance(instance_name)

        instance.unregister_message_client(job_name, message_queue)

        if self._is_instrumented:
            if self.__peers[peer] > 1:
                self.__peers[peer] -= 1
            else:
                self.__peers_by_instance[instance_name].remove(peer)
                del self.__peers[peer]

    def _get_instance(self, name):
        try:
            return self._instances[name]

        except KeyError:
            raise InvalidArgumentError("Instance doesn't exist on server: [{}]".format(name))
