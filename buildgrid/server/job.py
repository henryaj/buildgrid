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


from datetime import datetime
import logging
import uuid

from google.protobuf import duration_pb2, timestamp_pb2

from buildgrid._enums import LeaseState, OperationStage
from buildgrid._exceptions import CancelledError, NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid._protos.google.rpc import code_pb2


class Job:

    def __init__(self, action, action_digest, priority=0):
        self.__logger = logging.getLogger(__name__)

        self._name = str(uuid.uuid4())
        self._priority = priority
        self._action = remote_execution_pb2.Action()
        self._lease = None

        self.__execute_response = None
        self.__operation_metadata = remote_execution_pb2.ExecuteOperationMetadata()
        self.__operations_by_name = {}  # Name to Operation 1:1 mapping
        self.__operations_by_peer = {}  # Peer to Operation 1:1 mapping

        self.__queued_timestamp = timestamp_pb2.Timestamp()
        self.__queued_time_duration = duration_pb2.Duration()
        self.__worker_start_timestamp = timestamp_pb2.Timestamp()
        self.__worker_completed_timestamp = timestamp_pb2.Timestamp()

        self.__operations_message_queues = {}
        self.__operations_cancelled = set()
        self.__lease_cancelled = False
        self.__job_cancelled = False

        self.__operation_metadata.action_digest.CopyFrom(action_digest)
        self.__operation_metadata.stage = OperationStage.UNKNOWN.value

        self._action.CopyFrom(action)
        self._do_not_cache = self._action.do_not_cache
        self._n_tries = 0

        self._done = False

    def __lt__(self, other):
        try:
            return self.priority < other.priority
        except AttributeError:
            return NotImplemented

    def __le__(self, other):
        try:
            return self.priority <= other.priority
        except AttributeError:
            return NotImplemented

    def __eq__(self, other):
        if isinstance(other, Job):
            return self.name == other.name
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        try:
            return self.priority > other.priority
        except AttributeError:
            return NotImplemented

    def __ge__(self, other):
        try:
            return self.priority >= other.priority
        except AttributeError:
            return NotImplemented

    # --- Public API ---

    @property
    def name(self):
        return self._name

    @property
    def priority(self):
        return self._priority

    @property
    def done(self):
        return self._done

    # --- Public API: REAPI ---

    @property
    def do_not_cache(self):
        return self._do_not_cache

    @property
    def action_digest(self):
        return self.__operation_metadata.action_digest

    @property
    def operation_stage(self):
        return OperationStage(self.__operation_metadata.stage)

    @property
    def action_result(self):
        if self.__execute_response is not None:
            return self.__execute_response.result
        else:
            return None

    @property
    def holds_cached_result(self):
        if self.__execute_response is not None:
            return self.__execute_response.cached_result
        else:
            return False

    def set_cached_result(self, action_result):
        """Allows specifying an action result form the action cache for the job.

        Note:
            This won't trigger any :class:`Operation` stage transition.

        Args:
            action_result (ActionResult): The result from cache.
        """
        self.__execute_response = remote_execution_pb2.ExecuteResponse()
        self.__execute_response.result.CopyFrom(action_result)
        self.__execute_response.cached_result = True

    @property
    def n_peers(self):
        return len(self.__operations_message_queues)

    def n_peers_for_operation(self, operation_name):
        return len([operation for operation in self.__operations_by_peer.values()
                    if operation.name == operation_name])

    def register_new_operation_peer(self, peer, message_queue):
        """Subscribes to a new job's :class:`Operation` stage changes.

        Args:
            peer (str): a unique string identifying the client.
            message_queue (queue.Queue): the event queue to register.

        Returns:
            str: The name of the subscribed :class:`Operation`.
        """
        new_operation = operations_pb2.Operation()
        # Copy state from first existing and non cancelled operation:
        for operation in self.__operations_by_name.values():
            if operation.name not in self.__operations_cancelled:
                new_operation.CopyFrom(operation)
                break

        new_operation.name = str(uuid.uuid4())

        self.__operations_by_name[new_operation.name] = new_operation
        self.__operations_by_peer[peer] = new_operation
        self.__operations_message_queues[peer] = message_queue

        self._send_operations_updates(peers=[peer])

        return new_operation.name

    def register_operation_peer(self, operation_name, peer, message_queue):
        """Subscribes to one of the job's :class:`Operation` stage changes.

        Args:
            operation_name (str): an existing operation's name to subscribe to.
            peer (str): a unique string identifying the client.
            message_queue (queue.Queue): the event queue to register.

        Returns:
            str: The name of the subscribed :class:`Operation`.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        try:
            operation = self.__operations_by_name[operation_name]

        except KeyError:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        self.__operations_by_peer[peer] = operation
        self.__operations_message_queues[peer] = message_queue

        self._send_operations_updates(peers=[peer])

    def unregister_operation_peer(self, operation_name, peer):
        """Unsubscribes to the job's :class:`Operation` stage change.

        Args:
            operation_name (str): an existing operation's name to unsubscribe from.
            peer (str): a unique string identifying the client.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        try:
            operation = self.__operations_by_name[operation_name]

        except KeyError:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        if peer in self.__operations_message_queues:
            del self.__operations_message_queues[peer]

        del self.__operations_by_peer[peer]

        # Drop the operation if nobody is watching it anymore:
        if operation not in self.__operations_by_peer.values():
            del self.__operations_by_name[operation.name]

            self.__operations_cancelled.discard(operation.name)

    def list_operations(self):
        """Lists the :class:`Operation` related to a job.

        Returns:
            list: A list of :class:`Operation` names.
        """
        return list(self.__operations_by_name.keys())

    def get_operation(self, operation_name):
        """Returns a copy of the the job's :class:`Operation`.

        Args:
            operation_name (str): the operation's name.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        try:
            operation = self.__operations_by_name[operation_name]

        except KeyError:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        return self._copy_operation(operation)

    def update_operation_stage(self, stage):
        """Operates a stage transition for the job's :class:`Operation`.

        Args:
            stage (OperationStage): the operation stage to transition to.
        """
        if stage.value == self.__operation_metadata.stage:
            return

        self.__operation_metadata.stage = stage.value

        if self.__operation_metadata.stage == OperationStage.QUEUED.value:
            if self.__queued_timestamp.ByteSize() == 0:
                self.__queued_timestamp.GetCurrentTime()
            self._n_tries += 1

        elif self.__operation_metadata.stage == OperationStage.EXECUTING.value:
            queue_in, queue_out = self.__queued_timestamp.ToDatetime(), datetime.now()
            self.__queued_time_duration.FromTimedelta(queue_out - queue_in)

        elif self.__operation_metadata.stage == OperationStage.COMPLETED.value:
            self._done = True

        self._send_operations_updates()

    def cancel_operation(self, operation_name):
        """Triggers a job's :class:`Operation` cancellation.

        This may cancel any job's :class:`Lease` that may have been issued.

        Args:
            operation_name (str): the operation's name.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        try:
            operation = self.__operations_by_name[operation_name]

        except KeyError:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        self.__operations_cancelled.add(operation.name)

        ongoing_operations = set(self.__operations_by_name.keys())
        # Job is cancelled if all the operation are:
        self.__job_cancelled = ongoing_operations.issubset(self.__operations_cancelled)

        if self.__job_cancelled and self._lease is not None:
            self.cancel_lease()

        peers_to_notify = set()
        # If the job is not cancelled, notify all the peers watching the given
        # operation; if the job is cancelled, only notify the peers for which
        # the operation status changed.
        for peer, operation in self.__operations_by_peer.items():
            if self.__job_cancelled:
                if operation.name not in self.__operations_cancelled:
                    peers_to_notify.add(peer)
                elif operation.name == operation_name:
                    peers_to_notify.add(peer)

            else:
                if operation.name == operation_name:
                    peers_to_notify.add(peer)

        self._send_operations_updates(peers=peers_to_notify, notify_cancelled=True)

    # --- Public API: RWAPI ---

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
    def lease_cancelled(self):
        return self.__lease_cancelled

    @property
    def n_tries(self):
        return self._n_tries

    def create_lease(self):
        """Emits a new :class:`Lease` for the job.

        Only one :class:`Lease` can be emitted for a given job. This method
        should only be used once, any further calls are ignored.
        """
        if self._lease is not None:
            return self._lease
        elif self.__job_cancelled:
            return None

        self._lease = bots_pb2.Lease()
        self._lease.id = self._name
        self._lease.payload.Pack(self.__operation_metadata.action_digest)
        self._lease.state = LeaseState.PENDING.value

        return self._lease

    def update_lease_state(self, state, status=None, result=None):
        """Operates a state transition for the job's current :class:`Lease`.

        Args:
            state (LeaseState): the lease state to transition to.
            status (google.rpc.Status, optional): the lease execution status,
                only required if `state` is `COMPLETED`.
            result (google.protobuf.Any, optional): the lease execution result,
                only required if `state` is `COMPLETED`.
        """
        if state.value == self._lease.state:
            return

        self._lease.state = state.value

        if self._lease.state == LeaseState.PENDING.value:
            self.__worker_start_timestamp.Clear()
            self.__worker_completed_timestamp.Clear()

            self._lease.status.Clear()
            self._lease.result.Clear()

        elif self._lease.state == LeaseState.ACTIVE.value:
            self.__worker_start_timestamp.GetCurrentTime()

        elif self._lease.state == LeaseState.COMPLETED.value:
            self.__worker_completed_timestamp.GetCurrentTime()

            action_result = remote_execution_pb2.ActionResult()

            # TODO: Make a distinction between build and bot failures!
            if status.code != code_pb2.OK:
                self._do_not_cache = True

            if result is not None:
                assert result.Is(action_result.DESCRIPTOR)
                result.Unpack(action_result)

            action_metadata = action_result.execution_metadata
            action_metadata.queued_timestamp.CopyFrom(self.__queued_timestamp)
            action_metadata.worker_start_timestamp.CopyFrom(self.__worker_start_timestamp)
            action_metadata.worker_completed_timestamp.CopyFrom(self.__worker_completed_timestamp)

            self.__execute_response = remote_execution_pb2.ExecuteResponse()
            self.__execute_response.result.CopyFrom(action_result)
            self.__execute_response.cached_result = False
            self.__execute_response.status.CopyFrom(status)

    def cancel_lease(self):
        """Triggers a job's :class:`Lease` cancellation.

        Note:
            This will not cancel the job's :class:`Operation`.
        """
        self.__lease_cancelled = True
        if self._lease is not None:
            self.update_lease_state(LeaseState.CANCELLED)

    def delete_lease(self):
        """Discard the job's :class:`Lease`.

        Note:
            This will not cancel the job's :class:`Operation`.
        """
        self.__worker_start_timestamp.Clear()
        self.__worker_completed_timestamp.Clear()

        self._lease = None

    # --- Public API: Monitoring ---

    def query_queue_time(self):
        return self.__queued_time_duration.ToTimedelta()

    def query_n_retries(self):
        return self._n_tries - 1 if self._n_tries > 0 else 0

    # --- Private API ---

    def _copy_operation(self, operation):
        """Simply duplicates a given :class:`Lease` object."""
        new_operation = operations_pb2.Operation()
        new_operation.CopyFrom(operation)

        return new_operation

    def _update_operation(self, operation, operation_metadata, execute_response=None, done=False):
        """Forges a :class:`Operation` message given input data."""
        operation.metadata.Pack(operation_metadata)

        if execute_response is not None:
            operation.response.Pack(execute_response)

        operation.done = done

    def _update_cancelled_operation(self, operation, operation_metadata, execute_response=None):
        """Forges a cancelled :class:`Operation` message given input data."""
        cancelled_operation_metadata = remote_execution_pb2.ExecuteOperationMetadata()
        cancelled_operation_metadata.CopyFrom(operation_metadata)
        cancelled_operation_metadata.stage = OperationStage.COMPLETED.value

        operation.metadata.Pack(cancelled_operation_metadata)

        cancelled_execute_response = remote_execution_pb2.ExecuteResponse()
        if execute_response is not None:
            cancelled_execute_response.CopyFrom(self.__execute_response)
        cancelled_execute_response.status.code = code_pb2.CANCELLED
        cancelled_execute_response.status.message = "Operation cancelled by client."

        operation.response.Pack(cancelled_execute_response)

        operation.done = True

    def _send_operations_updates(self, peers=None, notify_cancelled=False):
        """Sends :class:`Operation` stage change messages to watchers."""
        for operation in self.__operations_by_name.values():
            if operation.name in self.__operations_cancelled:
                self._update_cancelled_operation(operation, self.__operation_metadata,
                                                 execute_response=self.__execute_response)

            else:
                self._update_operation(operation, self.__operation_metadata,
                                       execute_response=self.__execute_response,
                                       done=self._done)

        for peer, message_queue in self.__operations_message_queues.items():
            if peer not in self.__operations_by_peer:
                continue
            elif peers and peer not in peers:
                continue

            operation = self.__operations_by_peer[peer]
            # Messages are pairs of (Exception, Operation,):
            if not notify_cancelled and operation.name in self.__operations_cancelled:
                continue
            elif operation.name not in self.__operations_cancelled:
                message = (None, self._copy_operation(operation),)
            else:
                message = (CancelledError("Operation has been cancelled"),
                           self._copy_operation(operation),)

            message_queue.put(message)
