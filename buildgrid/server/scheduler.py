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
Scheduler
=========
Schedules jobs.
"""

from collections import deque
from datetime import timedelta
import logging

from buildgrid._enums import LeaseState, OperationStage
from buildgrid._exceptions import NotFoundError


class Scheduler:

    MAX_N_TRIES = 5

    def __init__(self, action_cache=None, monitor=False):
        self.__logger = logging.getLogger(__name__)

        self.__build_metadata_queues = None

        self.__operations_by_stage = None
        self.__leases_by_state = None
        self.__queue_time_average = None
        self.__retries_count = 0

        self._action_cache = action_cache
        self.jobs = {}
        self.queue = deque()

        self._is_instrumented = monitor

        if self._is_instrumented:
            self.activate_monitoring()

    # --- Public API ---

    def register_client(self, job_name, queue):
        job = self.jobs[job_name]

        job.register_client(queue)

    def unregister_client(self, job_name, queue):
        job = self.jobs[job_name]

        job.unregister_client(queue)

        if not job.n_clients and job.operation.done and not job.lease:
            del self.jobs[job_name]

            if self._is_instrumented:
                self.__operations_by_stage[OperationStage.CACHE_CHECK].discard(job_name)
                self.__operations_by_stage[OperationStage.QUEUED].discard(job_name)
                self.__operations_by_stage[OperationStage.EXECUTING].discard(job_name)
                self.__operations_by_stage[OperationStage.COMPLETED].discard(job_name)

                self.__leases_by_state[LeaseState.PENDING].discard(job_name)
                self.__leases_by_state[LeaseState.ACTIVE].discard(job_name)
                self.__leases_by_state[LeaseState.COMPLETED].discard(job_name)

    def queue_job(self, job, skip_cache_lookup=False):
        self.jobs[job.name] = job

        operation_stage = None
        if self._action_cache is not None and not skip_cache_lookup:
            try:
                action_result = self._action_cache.get_action_result(job.action_digest)
            except NotFoundError:
                operation_stage = OperationStage.QUEUED
                self.queue.append(job)

            else:
                job.set_cached_result(action_result)
                operation_stage = OperationStage.COMPLETED

                if self._is_instrumented:
                    self.__retries_count += 1

        else:
            operation_stage = OperationStage.QUEUED
            self.queue.append(job)

        self._update_job_operation_stage(job.name, operation_stage)

    def retry_job(self, job_name):
        job = self.jobs[job_name]

        operation_stage = None
        if job.n_tries >= self.MAX_N_TRIES:
            # TODO: Decide what to do with these jobs
            operation_stage = OperationStage.COMPLETED
            # TODO: Mark these jobs as done

        else:
            operation_stage = OperationStage.QUEUED
            job.update_lease_state(LeaseState.PENDING)
            self.queue.append(job)

        self._update_job_operation_stage(job_name, operation_stage)

    def list_jobs(self):
        return self.jobs.values()

    def request_job_leases(self, worker_capabilities):
        """Generates a list of the highest priority leases to be run.

        Args:
            worker_capabilities (dict): a set of key-value pairs decribing the
                worker properties, configuration and state at the time of the
                request.
        """
        if not self.queue:
            return []

        job = self.queue.popleft()

        lease = job.lease

        if not lease:
            # For now, one lease at a time:
            lease = job.create_lease()

        if lease:
            return [lease]

        return None

    def update_job_lease(self, lease):
        """Requests a state transition for a job's current :class:Lease.

        Args:
            job_name (str): name of the job to query.
            lease_state (LeaseState): the lease state to transition to.
            lease_status (google.rpc.Status): the lease execution status, only
                required if `lease_state` is `COMPLETED`.
            lease_result (google.protobuf.Any): the lease execution result, only
                required if `lease_state` is `COMPLETED`.
        """
        job = self.jobs[lease.id]
        lease_state = LeaseState(lease.state)

        operation_stage = None
        if lease_state == LeaseState.PENDING:
            job.update_lease_state(LeaseState.PENDING)
            operation_stage = OperationStage.QUEUED

            if self._is_instrumented:
                self.__leases_by_state[LeaseState.PENDING].add(lease.id)
                self.__leases_by_state[LeaseState.ACTIVE].discard(lease.id)
                self.__leases_by_state[LeaseState.COMPLETED].discard(lease.id)

        elif lease_state == LeaseState.ACTIVE:
            job.update_lease_state(LeaseState.ACTIVE)
            operation_stage = OperationStage.EXECUTING

            if self._is_instrumented:
                self.__leases_by_state[LeaseState.PENDING].discard(lease.id)
                self.__leases_by_state[LeaseState.ACTIVE].add(lease.id)
                self.__leases_by_state[LeaseState.COMPLETED].discard(lease.id)

        elif lease_state == LeaseState.COMPLETED:
            job.update_lease_state(LeaseState.COMPLETED,
                                   status=lease.status, result=lease.result)

            if self._action_cache is not None and not job.do_not_cache:
                self._action_cache.update_action_result(job.action_digest, job.action_result)

            operation_stage = OperationStage.COMPLETED

            if self._is_instrumented:
                self.__leases_by_state[LeaseState.PENDING].discard(lease.id)
                self.__leases_by_state[LeaseState.ACTIVE].discard(lease.id)
                self.__leases_by_state[LeaseState.COMPLETED].add(lease.id)

        self._update_job_operation_stage(lease.id, operation_stage)

    def get_job_lease(self, job_name):
        """Returns the lease associated to job, if any have been emitted yet."""
        return self.jobs[job_name].lease

    def get_job_lease_cancelled(self, job_name):
        """Returns true if the lease is cancelled"""
        return self.jobs[job_name].lease_cancelled

    def get_job_operation(self, job_name):
        """Returns the operation associated to job."""
        return self.jobs[job_name].operation

    def cancel_job_operation(self, job_name):
        """"Cancels the underlying operation of a given job.

        This will also cancel any job's lease that may have been issued.

        Args:
            job_name (str): name of the job holding the operation to cancel.
        """
        self.jobs[job_name].cancel_operation()

    # --- Public API: Monitoring ---

    @property
    def is_instrumented(self):
        return self._is_instrumented

    def activate_monitoring(self):
        """Activated jobs monitoring."""
        if self._is_instrumented:
            return

        self.__build_metadata_queues = []

        self.__operations_by_stage = {}
        self.__leases_by_state = {}
        self.__queue_time_average = 0, timedelta()
        self.__retries_count = 0

        self.__operations_by_stage[OperationStage.CACHE_CHECK] = set()
        self.__operations_by_stage[OperationStage.QUEUED] = set()
        self.__operations_by_stage[OperationStage.EXECUTING] = set()
        self.__operations_by_stage[OperationStage.COMPLETED] = set()

        self.__leases_by_state[LeaseState.PENDING] = set()
        self.__leases_by_state[LeaseState.ACTIVE] = set()
        self.__leases_by_state[LeaseState.COMPLETED] = set()

        self._is_instrumented = True

    def deactivate_monitoring(self):
        """Deactivated jobs monitoring."""
        if not self._is_instrumented:
            return

        self._is_instrumented = False

        self.__build_metadata_queues = None

        self.__operations_by_stage = None
        self.__leases_by_state = None
        self.__queue_time_average = None
        self.__retries_count = 0

    def register_build_metadata_watcher(self, message_queue):
        if self.__build_metadata_queues is not None:
            self.__build_metadata_queues.append(message_queue)

    def query_n_jobs(self):
        return len(self.jobs)

    def query_n_operations(self):
        # For now n_operations == n_jobs:
        return len(self.jobs)

    def query_n_operations_by_stage(self, operation_stage):
        try:
            if self.__operations_by_stage is not None:
                return len(self.__operations_by_stage[operation_stage])
        except KeyError:
            pass
        return 0

    def query_n_leases(self):
        return len(self.jobs)

    def query_n_leases_by_state(self, lease_state):
        try:
            if self.__leases_by_state is not None:
                return len(self.__leases_by_state[lease_state])
        except KeyError:
            pass
        return 0

    def query_n_retries(self):
        return self.__retries_count

    def query_am_queue_time(self):
        if self.__queue_time_average is not None:
            return self.__queue_time_average[1]
        return timedelta()

    # --- Private API ---

    def _update_job_operation_stage(self, job_name, operation_stage):
        """Requests a stage transition for the job's :class:Operations.

        Args:
            job_name (str): name of the job to query.
            operation_stage (OperationStage): the stage to transition to.
        """
        job = self.jobs[job_name]

        if operation_stage == OperationStage.CACHE_CHECK:
            job.update_operation_stage(OperationStage.CACHE_CHECK)

            if self._is_instrumented:
                self.__operations_by_stage[OperationStage.CACHE_CHECK].add(job_name)
                self.__operations_by_stage[OperationStage.QUEUED].discard(job_name)
                self.__operations_by_stage[OperationStage.EXECUTING].discard(job_name)
                self.__operations_by_stage[OperationStage.COMPLETED].discard(job_name)

        elif operation_stage == OperationStage.QUEUED:
            job.update_operation_stage(OperationStage.QUEUED)

            if self._is_instrumented:
                self.__operations_by_stage[OperationStage.CACHE_CHECK].discard(job_name)
                self.__operations_by_stage[OperationStage.QUEUED].add(job_name)
                self.__operations_by_stage[OperationStage.EXECUTING].discard(job_name)
                self.__operations_by_stage[OperationStage.COMPLETED].discard(job_name)

        elif operation_stage == OperationStage.EXECUTING:
            job.update_operation_stage(OperationStage.EXECUTING)

            if self._is_instrumented:
                self.__operations_by_stage[OperationStage.CACHE_CHECK].discard(job_name)
                self.__operations_by_stage[OperationStage.QUEUED].discard(job_name)
                self.__operations_by_stage[OperationStage.EXECUTING].add(job_name)
                self.__operations_by_stage[OperationStage.COMPLETED].discard(job_name)

        elif operation_stage == OperationStage.COMPLETED:
            job.update_operation_stage(OperationStage.COMPLETED)

            if self._is_instrumented:
                self.__operations_by_stage[OperationStage.CACHE_CHECK].discard(job_name)
                self.__operations_by_stage[OperationStage.QUEUED].discard(job_name)
                self.__operations_by_stage[OperationStage.EXECUTING].discard(job_name)
                self.__operations_by_stage[OperationStage.COMPLETED].add(job_name)

                average_order, average_time = self.__queue_time_average

                average_order += 1
                if average_order <= 1:
                    average_time = job.query_queue_time()
                else:
                    queue_time = job.query_queue_time()
                    average_time = average_time + ((queue_time - average_time) / average_order)

                self.__queue_time_average = average_order, average_time

                if not job.holds_cached_action_result:
                    execution_metadata = job.action_result.execution_metadata
                    context_metadata = {'job-is': job.name}

                    message = (execution_metadata, context_metadata,)

                    for message_queue in self.__build_metadata_queues:
                        message_queue.put(message)
