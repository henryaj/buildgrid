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


import logging
import uuid
from enum import Enum

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.longrunning import operations_pb2


class OperationStage(Enum):
    # Initially unknown stage.
    UNKNOWN = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('UNKNOWN')
    # Checking the result against the cache.
    CACHE_CHECK = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('CACHE_CHECK')
    # Currently idle, awaiting a free machine to execute.
    QUEUED = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('QUEUED')
    # Currently being executed by a worker.
    EXECUTING = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('EXECUTING')
    # Finished execution.
    COMPLETED = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('COMPLETED')


class LeaseState(Enum):
    # Initially unknown state.
    LEASE_STATE_UNSPECIFIED = bots_pb2.LeaseState.Value('LEASE_STATE_UNSPECIFIED')
    # The server expects the bot to accept this lease.
    PENDING = bots_pb2.LeaseState.Value('PENDING')
    # The bot has accepted this lease.
    ACTIVE = bots_pb2.LeaseState.Value('ACTIVE')
    # The bot is no longer leased.
    COMPLETED = bots_pb2.LeaseState.Value('COMPLETED')
    # The bot should immediately release all resources associated with the lease.
    CANCELLED = bots_pb2.LeaseState.Value('CANCELLED')


class Job:

    def __init__(self, action_digest, do_not_cache=False, message_queue=None):
        self.logger = logging.getLogger(__name__)

        self._name = str(uuid.uuid4())
        self._operation = operations_pb2.Operation()
        self._lease = None

        self.__execute_response = None
        self.__operation_metadata = remote_execution_pb2.ExecuteOperationMetadata()

        self.__operation_metadata.action_digest.CopyFrom(action_digest)
        self.__operation_metadata.stage = OperationStage.UNKNOWN.value

        self._do_not_cache = do_not_cache
        self._operation_update_queues = []
        self._operation.name = self._name
        self._operation.done = False
        self._n_tries = 0

        if message_queue is not None:
            self.register_client(message_queue)

    @property
    def name(self):
        return self._name

    @property
    def do_not_cache(self):
        return self._do_not_cache

    @property
    def action_digest(self):
        return self.__operation_metadata.action_digest

    @property
    def action_result(self):
        if self.__execute_response is not None:
            return self.__execute_response.result
        else:
            return None

    @property
    def operation(self):
        return self._operation

    @property
    def operation_stage(self):
        return OperationStage(self.__operation_metadata.state)

    @property
    def lease(self):
        return self._lease

    @property
    def lease_state(self):
        if self._lease is not None:
            return LeaseState(self._lease.state)
        else:
            return None

    @property
    def n_tries(self):
        return self._n_tries

    @property
    def n_clients(self):
        return len(self._operation_update_queues)

    def register_client(self, queue):
        """Subscribes to the job's :class:`Operation` stage change events.

        Args:
            queue (queue.Queue): the event queue to register.
        """
        self._operation_update_queues.append(queue)
        queue.put(self._operation)

    def unregister_client(self, queue):
        """Unsubscribes to the job's :class:`Operation` stage change events.

        Args:
            queue (queue.Queue): the event queue to unregister.
        """
        self._operation_update_queues.remove(queue)

    def set_cached_result(self, action_result):
        """Allows specifying an action result form the action cache for the job.
        """
        self.__execute_response = remote_execution_pb2.ExecuteResponse()
        self.__execute_response.result.CopyFrom(action_result)
        self.__execute_response.cached_result = True

    def create_lease(self):
        """Emits a new :class:`Lease` for the job.

        Only one :class:`Lease` can be emitted for a given job. This method
        should only be used once, any furhter calls are ignored.
        """
        if self._lease is not None:
            return None

        self._lease = bots_pb2.Lease()
        self._lease.id = self._name
        self._lease.payload.Pack(self.__operation_metadata.action_digest)
        self._lease.state = LeaseState.PENDING.value

        return self._lease

    def update_lease_state(self, state, status=None, result=None):
        """Operates a state transition for the job's current :class:Lease.

        Args:
            state (LeaseState): the lease state to transition to.
            status (google.rpc.Status): the lease execution status, only
                required if `state` is `COMPLETED`.
            result (google.protobuf.Any): the lease execution result, only
                required if `state` is `COMPLETED`.
        """
        if state.value == self._lease.state:
            return

        self._lease.state = state.value

        if self._lease.state == LeaseState.PENDING.value:
            self._lease.status.Clear()
            self._lease.result.Clear()

        elif self._lease.state == LeaseState.COMPLETED.value:
            action_result = remote_execution_pb2.ActionResult()

            if result is not None:
                assert result.Is(action_result.DESCRIPTOR)
                result.Unpack(action_result)

            self.__execute_response = remote_execution_pb2.ExecuteResponse()
            self.__execute_response.result.CopyFrom(action_result)
            self.__execute_response.cached_result = False
            self.__execute_response.status.CopyFrom(status)

    def update_operation_stage(self, stage):
        """Operates a stage transition for the job's :class:Operation.

        Args:
            stage (OperationStage): the operation stage to transition to.
        """
        if stage.value == self.__operation_metadata.stage:
            return

        self.__operation_metadata.stage = stage.value

        if self.__operation_metadata.stage == OperationStage.QUEUED.value:
            self._n_tries += 1

        elif self.__operation_metadata.stage == OperationStage.COMPLETED.value:
            if self.__execute_response is not None:
                self._operation.response.Pack(self.__execute_response)
            self._operation.done = True

        self._operation.metadata.Pack(self.__operation_metadata)

        for queue in self._operation_update_queues:
            queue.put(self._operation)
