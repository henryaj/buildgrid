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

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from buildgrid._enums import LeaseState, OperationStage, BotStatus
from buildgrid._exceptions import CancelledError, NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid._protos.google.rpc import code_pb2
from buildgrid.server.persistence import DataStore


class Job:

    def __init__(self, do_not_cache, action_digest, platform_requirements=None, priority=0,
                 name=None, operations=(), lease=None, stage=OperationStage.UNKNOWN.value,
                 cancelled=False, queued_timestamp=Timestamp(), queued_time_duration=Duration(),
                 worker_start_timestamp=Timestamp(), worker_completed_timestamp=Timestamp(),
                 done=False, result=None):
        self.__logger = logging.getLogger(__name__)

        self._name = name or str(uuid.uuid4())
        self._priority = priority
        self._lease = lease

        self.__execute_response = result
        if result is None:
            self.__execute_response = remote_execution_pb2.ExecuteResponse()
        self.__operation_metadata = remote_execution_pb2.ExecuteOperationMetadata()

        self.__queued_timestamp = Timestamp()
        self.__queued_timestamp.CopyFrom(queued_timestamp)

        self.__queued_time_duration = Duration()
        self.__queued_time_duration.CopyFrom(queued_time_duration)

        self.__worker_start_timestamp = Timestamp()
        self.__worker_start_timestamp.CopyFrom(worker_start_timestamp)
        self.__worker_completed_timestamp = Timestamp()
        self.__worker_completed_timestamp.CopyFrom(worker_completed_timestamp)

        self.__operations_by_name = {op.name: op for op in operations}  # Name to Operation 1:1 mapping
        self.__operations_cancelled = set()
        if cancelled:
            self.__operations_cancelled = set(op.name for op in operations)
        self.__lease_cancelled = cancelled
        self.__job_cancelled = cancelled

        self.__operation_metadata.action_digest.CopyFrom(action_digest)
        self.__operation_metadata.stage = stage

        self._do_not_cache = do_not_cache
        self._n_tries = 0

        self._platform_requirements = platform_requirements \
            if platform_requirements else dict()

        self._done = done

        DataStore.create_job(self)

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
    def cancelled(self):
        return self.__job_cancelled

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, new_priority):
        self._priority = new_priority
        changes = {'priority': new_priority}
        DataStore.update_job(self.name, changes)

    @property
    def done(self):
        return self._done

    # --- Public API: REAPI ---

    @property
    def platform_requirements(self):
        return self._platform_requirements

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
    def execute_response(self):
        return self.__execute_response

    @execute_response.setter
    def execute_response(self, response):
        self.__execute_response = response
        for operation in self.__operations_by_name.values():
            operation.response.Pack(self.__execute_response)

    @property
    def holds_cached_result(self):
        if self.__execute_response is not None:
            return self.__execute_response.cached_result
        else:
            return False

    @property
    def queued_timestamp(self):
        return self.__queued_timestamp

    @property
    def queued_time_duration(self):
        return self.__queued_time_duration

    @property
    def worker_start_timestamp(self):
        return self.__worker_start_timestamp

    @property
    def worker_completed_timestamp(self):
        return self.__worker_completed_timestamp

    def mark_worker_started(self):
        self.__worker_start_timestamp.GetCurrentTime()

    def set_action_url(self, url):
        """Generates a CAS browser URL for the job's action."""
        if url.for_message('action', self.__operation_metadata.action_digest):
            self.__execute_response.message = url.generate()

    def set_cached_result(self, action_result):
        """Allows specifying an action result form the action cache for the job.

        Note:
            This won't trigger any :class:`Operation` stage transition.

        Args:
            action_result (ActionResult): The result from cache.
        """
        self.__execute_response.result.CopyFrom(action_result)
        self.__execute_response.cached_result = True

    def n_peers(self, operations_by_peer):
        return len([peer for peer, names in operations_by_peer.items()
                    if any(name in self.__operations_by_name for name in names)])

    def n_peers_for_operation(self, operation_name, operations_by_peer):
        return len([peer for peer, names in operations_by_peer.items()
                    if any(name == operation_name for name in names)])

    def register_new_operation_peer(self, peer, message_queue, operations_by_peer, peer_message_queues):
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

        self.__logger.debug("Operation created for job [%s]: [%s]",
                            self._name, new_operation.name)

        self.__operations_by_name[new_operation.name] = new_operation
        if peer in operations_by_peer:
            operations_by_peer[peer].add(new_operation.name)
        else:
            operations_by_peer[peer] = set([new_operation.name])

        if peer in peer_message_queues:
            peer_message_queues[peer][new_operation.name] = message_queue
        else:
            peer_message_queues[peer] = {new_operation.name: message_queue}

        DataStore.create_operation(new_operation, self._name)

        self._send_operations_updates(peers=[peer],
                                      operations_by_peer=operations_by_peer,
                                      peer_message_queues=peer_message_queues)

        return new_operation.name

    def register_operation_peer(self, operation_name, peer, message_queue, operations_by_peer, peer_message_queues):
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
        if operation_name not in self.__operations_by_name:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        if peer in operations_by_peer:
            operations_by_peer[peer].add(operation_name)
        else:
            operations_by_peer[peer] = set([operation_name])

        if peer in peer_message_queues:
            peer_message_queues[peer][operation_name] = message_queue
        else:
            peer_message_queues[peer] = {operation_name: message_queue}

        self._send_operations_updates(peers=[peer],
                                      operations_by_peer=operations_by_peer,
                                      peer_message_queues=peer_message_queues)

    def unregister_operation_peer(self, operation_name, peer):
        """Unsubscribes to the job's :class:`Operation` stage change.

        Args:
            operation_name (str): an existing operation's name to unsubscribe from.
            peer (str): a unique string identifying the client.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        if operation_name not in self.__operations_by_name:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

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

    def update_operation_stage(self, stage, operations_by_peer, peer_message_queues):
        """Operates a stage transition for the job's :class:`Operation`.

        Args:
            stage (OperationStage): the operation stage to transition to.
        """
        if stage.value == self.__operation_metadata.stage:
            return

        changes = {}

        self.__operation_metadata.stage = stage.value
        changes["stage"] = stage.value

        self.__logger.debug("Stage changed for job [%s]: [%s] (operation)",
                            self._name, stage.name)

        if self.__operation_metadata.stage == OperationStage.QUEUED.value:
            if self.__queued_timestamp.ByteSize() == 0:
                self.__queued_timestamp.GetCurrentTime()
                changes["queued_timestamp"] = self.__queued_timestamp.ToDatetime()
            self._n_tries += 1

        elif self.__operation_metadata.stage == OperationStage.EXECUTING.value:
            queue_in, queue_out = self.__queued_timestamp.ToDatetime(), datetime.utcnow()
            self.__queued_time_duration.FromTimedelta(queue_out - queue_in)
            changes["queued_time_duration"] = self.__queued_time_duration.seconds

        elif self.__operation_metadata.stage == OperationStage.COMPLETED.value:
            self._done = True

        DataStore.update_job(self.name, changes)

        self._send_operations_updates(operations_by_peer=operations_by_peer,
                                      peer_message_queues=peer_message_queues)

    def cancel_operation(self, operation_name, operations_by_peer, peer_message_queues):
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

        self.__logger.debug("Operation cancelled for job [%s]: [%s]",
                            self._name, operation.name)

        ongoing_operations = set(self.__operations_by_name.keys())
        # Job is cancelled if all the operation are:
        self.__job_cancelled = ongoing_operations.issubset(self.__operations_cancelled)

        if self.__job_cancelled:
            self.__operation_metadata.stage = OperationStage.COMPLETED.value
            DataStore.update_job(self.name, {"stage": OperationStage.COMPLETED.value})
            if self._lease is not None:
                self.cancel_lease()

        peers_to_notify = set()
        # If the job is not cancelled, notify all the peers watching the given
        # operation; if the job is cancelled, only notify the peers for which
        # the operation status changed.
        for peer, names in operations_by_peer.items():
            relevant_names = [n for n in names if n in self.__operations_by_name]
            if self.__job_cancelled:
                if not any(name in self.__operations_cancelled for name in relevant_names):
                    peers_to_notify.add(peer)
                elif operation_name in relevant_names:
                    peers_to_notify.add(peer)

            else:
                if operation_name in relevant_names:
                    peers_to_notify.add(peer)

        self._send_operations_updates(peers=peers_to_notify,
                                      notify_cancelled=True,
                                      operations_by_peer=operations_by_peer,
                                      peer_message_queues=peer_message_queues)

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
        self._lease.state = LeaseState.UNSPECIFIED.value

        self.__logger.debug("Lease created for job [%s]: [%s]",
                            self._name, self._lease.id)

        self.update_lease_state(LeaseState.PENDING, skip_lease_persistence=True)

        return self._lease

    def update_lease_state(self, state, status=None, result=None, skip_lease_persistence=False):
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

        job_changes = {}
        lease_changes = {}

        self._lease.state = state.value
        lease_changes["state"] = state.value

        self.__logger.debug("State changed for job [%s]: [%s] (lease)",
                            self._name, state.name)

        if self._lease.state == LeaseState.PENDING.value:
            self.__worker_start_timestamp.Clear()
            self.__worker_completed_timestamp.Clear()
            job_changes["worker_start_timestamp"] = self.__worker_start_timestamp.ToDatetime()
            job_changes["worker_completed_timestamp"] = self.__worker_completed_timestamp.ToDatetime()

            self._lease.status.Clear()
            self._lease.result.Clear()
            lease_changes["status"] = self._lease.status.code

        elif self._lease.state == LeaseState.COMPLETED.value:
            self.__worker_completed_timestamp.GetCurrentTime()
            job_changes["worker_completed_timestamp"] = self.__worker_completed_timestamp.ToDatetime()

            action_result = remote_execution_pb2.ActionResult()

            # TODO: Make a distinction between build and bot failures!
            if status.code != code_pb2.OK:
                self._do_not_cache = True
                job_changes["do_not_cache"] = True

            lease_changes["status"] = status.code

            if result is not None and result.Is(action_result.DESCRIPTOR):
                result.Unpack(action_result)

            action_metadata = action_result.execution_metadata
            action_metadata.queued_timestamp.CopyFrom(self.__queued_timestamp)
            action_metadata.worker_start_timestamp.CopyFrom(self.__worker_start_timestamp)
            action_metadata.worker_completed_timestamp.CopyFrom(self.__worker_completed_timestamp)

            self.__execute_response.result.CopyFrom(action_result)
            self.__execute_response.cached_result = False
            self.__execute_response.status.CopyFrom(status)

        DataStore.update_job(self.name, job_changes)
        if not skip_lease_persistence:
            DataStore.update_lease(self.name, lease_changes)

    def cancel_lease(self):
        """Triggers a job's :class:`Lease` cancellation.

        Note:
            This will not cancel the job's :class:`Operation`.
        """
        self.__lease_cancelled = True

        self.__logger.debug("Lease cancelled for job [%s]: [%s]",
                            self._name, self._lease.id)

        if self._lease is not None:
            self.update_lease_state(LeaseState.CANCELLED)

    def delete_lease(self):
        """Discard the job's :class:`Lease`.

        Note:
            This will not cancel the job's :class:`Operation`.
        """
        if self._lease is not None:
            self.__worker_start_timestamp.Clear()
            self.__worker_completed_timestamp.Clear()

            self.__logger.debug("Lease deleted for job [%s]: [%s]",
                                self._name, self._lease.id)

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
        changes = {"done": done}
        DataStore.update_operation(operation.name, changes)

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
        changes = {"done": True}
        DataStore.update_operation(operation.name, changes)

    def _send_operations_updates(self, peers=None, notify_cancelled=False,
                                 operations_by_peer=None, peer_message_queues=None):
        """Sends :class:`Operation` stage change messages to watchers."""
        if operations_by_peer is None:
            operations_by_peer = {}
        if peer_message_queues is None:
            peer_message_queues = {}

        for operation in self.__operations_by_name.values():
            if operation.name in self.__operations_cancelled:
                self._update_cancelled_operation(operation, self.__operation_metadata,
                                                 execute_response=self.__execute_response)

            else:
                self._update_operation(operation, self.__operation_metadata,
                                       execute_response=self.__execute_response,
                                       done=self._done)

        relevant_queues = {peer: mqs for peer, mqs in peer_message_queues.items()
                           if any(name in self.__operations_by_name
                                  for name in mqs)}

        for peer, message_queues in relevant_queues.items():
            if peer not in operations_by_peer:
                continue
            elif peers and peer not in peers:
                continue

            operations = [self.__operations_by_name[name] for name in operations_by_peer[peer]
                          if name in self.__operations_by_name]
            for operation in operations:
                # Messages are pairs of (Exception, Operation,):
                if not notify_cancelled and operation.name in self.__operations_cancelled:
                    continue
                elif operation.name not in self.__operations_cancelled:
                    message = (None, self._copy_operation(operation),)
                else:
                    message = (CancelledError("Operation has been cancelled"),
                               self._copy_operation(operation),)

                message_queue = message_queues[operation.name]
                message_queue.put(message)
