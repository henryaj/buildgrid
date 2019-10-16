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
================

Serves remote execution requests.
"""

import logging
import queue
from functools import partial

import grpc

from buildgrid._exceptions import FailedPreconditionError, InvalidArgumentError, CancelledError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid.server._authentication import AuthContext, authorize
from buildgrid.server.peer import Peer
from buildgrid.server._resources import ExecContext, limit


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
        """Registers a new servicer instance.

        Args:
            instance_name (str): The new instance's name.
            instance (ExecutionInstance): The new instance itself.
        """
        self._instances[instance_name] = instance

        if self._is_instrumented:
            self.__peers_by_instance[instance_name] = set()

    def get_scheduler(self, instance_name):
        """Retrieves a reference to the scheduler for an instance.

        Args:
            instance_name (str): The name of the instance to query.

        Returns:
            Scheduler: A reference to the scheduler for `instance_name`.

        Raises:
            InvalidArgumentError: If no instance named `instance_name` exists.
        """
        instance = self._get_instance(instance_name)

        return instance.scheduler

    # --- Public API: Servicer ---

    @authorize(AuthContext)
    @limit(ExecContext)
    def Execute(self, request, context):
        """Handles ExecuteRequest messages.

        Args:
            request (ExecuteRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.debug("Execute request from [%s]", context.peer())

        instance_name = request.instance_name
        message_queue = queue.Queue()
        peer_uid = context.peer()

        Peer.register_peer(uid=peer_uid, context=context)

        try:
            instance = self._get_instance(instance_name)

            job_name = instance.execute(request.action_digest,
                                        request.skip_cache_lookup)

            operation_name = instance.register_job_peer(job_name,
                                                        peer_uid, message_queue)

            context.add_callback(partial(self._rpc_termination_callback,
                                         peer_uid, instance_name, operation_name))

            if self._is_instrumented:
                if peer_uid not in self.__peers:
                    self.__peers_by_instance[instance_name].add(peer_uid)
                    self.__peers[peer_uid] = 1
                else:
                    self.__peers[peer_uid] += 1

            operation_full_name = "{}/{}".format(instance_name, operation_name)

            self.__logger.info("Operation [%s] created for job [%s]",
                               operation_full_name, job_name)

            for operation in instance.stream_operation_updates_while_context_is_active(message_queue, context):
                operation.name = operation_full_name
                yield operation

            if not context.is_active():
                self.__logger.info("Peer peer_uid=[%s] was holding up a thread for "
                                   " `stream_operation_updates()` for instance_name=[%s],"
                                   " operation_name=[%s], but the rpc context is not active anymore;"
                                   " releasing thread.",
                                   peer_uid, instance_name, operation_name)

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
            self.__logger.info("Operation cancelled [%s]", operation_full_name)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.CANCELLED)
            yield e.last_response

    @authorize(AuthContext)
    @limit(ExecContext)
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

            instance.register_operation_peer(operation_name,
                                             peer, message_queue)

            context.add_callback(partial(self._rpc_termination_callback,
                                         peer, instance_name, operation_name))

            if self._is_instrumented:
                if peer not in self.__peers:
                    self.__peers_by_instance[instance_name].add(peer)
                    self.__peers[peer] = 1
                else:
                    self.__peers[peer] += 1

            operation_full_name = "{}/{}".format(instance_name, operation_name)

            for operation in instance.stream_operation_updates_while_context_is_active(message_queue, context):
                operation.name = operation_full_name
                yield operation

            if not context.is_active():
                self.__logger.info("Peer peer_uid=[%s] was holding up a thread for "
                                   " `stream_operation_updates()` for instance_name=[%s],"
                                   " operation_name=[%s], but the rpc context is not active anymore;"
                                   " releasing thread.",
                                   peer, instance_name, operation_name)

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield operations_pb2.Operation()

        except CancelledError as e:
            self.__logger.info("Operation cancelled [%s]", operation_full_name)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.CANCELLED)
            yield e.last_response

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

    def _rpc_termination_callback(self, peer_uid, instance_name, operation_name):
        self.__logger.debug("RPC terminated for peer_uid=[%s], instance_name=[%s], operation_name=[%s]",
                            peer_uid, instance_name, operation_name)

        instance = self._get_instance(instance_name)

        instance.unregister_operation_peer(operation_name, peer_uid)

        if self._is_instrumented:
            if self.__peers[peer_uid] > 1:
                self.__peers[peer_uid] -= 1
            else:
                self.__peers_by_instance[instance_name].remove(peer_uid)
                del self.__peers[peer_uid]

        Peer.deregister_peer(peer_uid)

    def _get_instance(self, name):
        try:
            return self._instances[name]

        except KeyError:
            raise InvalidArgumentError("Instance doesn't exist on server: [{}]".format(name))
