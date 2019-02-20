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

import bisect
from datetime import timedelta
import logging
from threading import Lock

from buildgrid._enums import LeaseState, OperationStage
from buildgrid._exceptions import NotFoundError
from buildgrid.server.job import Job
from buildgrid.utils import BrowserURL


class Scheduler:

    MAX_N_TRIES = 5

    def __init__(self, action_cache=None, action_browser_url=False, monitor=False):
        self.__logger = logging.getLogger(__name__)

        self._instance_name = None

        self.__build_metadata_queues = None

        self.__operations_by_stage = None
        self.__leases_by_state = None
        self.__queue_time_average = None
        self.__retries_count = 0

        self._action_cache = action_cache
        self._action_browser_url = action_browser_url

        self.__jobs_by_action = {}  # Action to Job 1:1 mapping
        self.__jobs_by_operation = {}  # Operation to Job 1:1 mapping
        self.__jobs_by_name = {}  # Name to Job 1:1 mapping

        self.__queue = []
        self.__queue_lock = Lock()

        self._is_instrumented = False
        if monitor:
            self.activate_monitoring()

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    def set_instance_name(self, instance_name):
        if not self._instance_name:
            self._instance_name = instance_name

    def list_current_jobs(self):
        """Returns a list of the :class:`Job` names currently managed."""
        return self.__jobs_by_name.keys()

    def list_job_operations(self, job_name):
        """Returns a list of :class:`Operation` names for a :class:`Job`."""
        if job_name in self.__jobs_by_name:
            return self.__jobs_by_name[job_name].list_operations()
        else:
            return []

    # --- Public API: REAPI ---

    def register_job_peer(self, job_name, peer, message_queue):
        """Subscribes to the job's :class:`Operation` stage changes.

        Args:
            job_name (str): name of the job to subscribe to.
            peer (str): a unique string identifying the client.
            message_queue (queue.Queue): the event queue to register.

        Returns:
            str: The name of the subscribed :class:`Operation`.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        try:
            job = self.__jobs_by_name[job_name]

        except KeyError:
            raise NotFoundError("Job name does not exist: [{}]"
                                .format(job_name))

        operation_name = job.register_new_operation_peer(peer, message_queue)

        self.__jobs_by_operation[operation_name] = job

        return operation_name

    def register_job_operation_peer(self, operation_name, peer, message_queue):
        """Subscribes to an existing the job's :class:`Operation` stage changes.

        Args:
            operation_name (str): name of the operation to subscribe to.
            peer (str): a unique string identifying the client.
            message_queue (queue.Queue): the event queue to register.

        Returns:
            str: The name of the subscribed :class:`Operation`.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        try:
            job = self.__jobs_by_operation[operation_name]

        except KeyError:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        job.register_operation_peer(operation_name, peer, message_queue)

    def unregister_job_operation_peer(self, operation_name, peer):
        """Unsubscribes to one of the job's :class:`Operation` stage change.

        Args:
            operation_name (str): name of the operation to unsubscribe from.
            peer (str): a unique string identifying the client.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        try:
            job = self.__jobs_by_operation[operation_name]

        except KeyError:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        job.unregister_operation_peer(operation_name, peer)

        if not job.n_peers_for_operation(operation_name):
            del self.__jobs_by_operation[operation_name]

        if not job.n_peers and job.done and not job.lease:
            self._delete_job(job.name)

    def queue_job_action(self, action, action_digest, platform_requirements=None,
                         priority=0, skip_cache_lookup=False):
        """Inserts a newly created job into the execution queue.

        Warning:
            Priority is handle like a POSIX ``nice`` values: a higher value
            means a low priority, 0 being default priority.

        Args:
            action (Action): the given action to queue for execution.
            action_digest (Digest): the digest of the given action.
            platform_requirements (dict(set)): platform attributes that a worker
                must satisfy in order to be assigned the job. (Each key can
                have multiple values.)
            priority (int): the execution job's priority.
            skip_cache_lookup (bool): whether or not to look for pre-computed
                result for the given action.

        Returns:
            str: the newly created job's name.
        """
        if action_digest.hash in self.__jobs_by_action:
            job = self.__jobs_by_action[action_digest.hash]
            # If existing job has been cancelled create a new one:
            if not job.cancelled:
                # Reschedule if priority is now greater:
                if priority < job.priority:
                    job.priority = priority

                    if job.operation_stage == OperationStage.QUEUED:
                        self._queue_job(job.name)

                self.__logger.debug("Job deduplicated for action [%s]: [%s]",
                                    action_digest.hash[:8], job.name)

                return job.name

        job = Job(action, action_digest,
                  platform_requirements=platform_requirements,
                  priority=priority)

        if self._action_browser_url:
            job.set_action_url(
                BrowserURL(self._action_browser_url, self._instance_name))

        self.__logger.debug("Job created for action [%s]: [%s]",
                            action_digest.hash[:8], job.name)

        self.__jobs_by_action[job.action_digest.hash] = job
        self.__jobs_by_name[job.name] = job

        operation_stage = None

        if self._action_cache is not None and not skip_cache_lookup:
            try:
                action_result = self._action_cache.get_action_result(job.action_digest)

                self.__logger.debug("Job cache hit for action [%s]: [%s]",
                                    action_digest.hash[:8], job.name)

                operation_stage = OperationStage.COMPLETED
                job.set_cached_result(action_result)

            except NotFoundError:
                operation_stage = OperationStage.QUEUED
                self._queue_job(job.name)

        else:
            operation_stage = OperationStage.QUEUED
            self._queue_job(job.name)

        self._update_job_operation_stage(job.name, operation_stage)

        return job.name

    def get_job_operation(self, operation_name):
        """Retrieves a job's :class:`Operation` by name.

        Args:
            operation_name (str): name of the operation to query.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        try:
            job = self.__jobs_by_operation[operation_name]

        except KeyError:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        return job.get_operation(operation_name)

    def cancel_job_operation(self, operation_name):
        """"Cancels a job's :class:`Operation` by name.

        Args:
            operation_name (str): name of the operation to cancel.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        try:
            job = self.__jobs_by_operation[operation_name]

        except KeyError:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        job.cancel_operation(operation_name)

    def delete_job_operation(self, operation_name):
        """"Removes a job.

        Args:
            operation_name (str): name of the operation to delete.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        try:
            job = self.__jobs_by_operation[operation_name]

        except KeyError:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        if not job.n_peers and job.done and not job.lease:
            self._delete_job(job.name)

    # --- Public API: RWAPI ---

    def request_job_leases(self, worker_capabilities):
        """Generates a list of the highest priority leases to be run.

        Args:
            worker_capabilities (dict): a set of key-value pairs describing the
                worker properties, configuration and state at the time of the
                request.
        """
        # TODO: Replace with a more efficient way of doing this.
        with self.__queue_lock:
            # Looking for the first job that could be assigned to the worker...
            for job_index, job in enumerate(self.__queue):
                if self._worker_is_capable(worker_capabilities, job):
                    self.__logger.info("Job scheduled to run: [%s]", job.name)

                    lease = job.lease

                    if not lease:
                        # For now, one lease at a time:
                        lease = job.create_lease()

                    if lease:
                        del self.__queue[job_index]
                        return [lease]

                    return []

            return []

    def update_job_lease_state(self, job_name, lease):
        """Requests a state transition for a job's current :class:Lease.

        Note:
            This may trigger a job's :class:`Operation` stage transition.

        Args:
            job_name (str): name of the job to update lease state from.
            lease (Lease): the lease holding the new state.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        try:
            job = self.__jobs_by_name[job_name]

        except KeyError:
            raise NotFoundError("Job name does not exist: [{}]".format(job_name))

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

        self._update_job_operation_stage(job_name, operation_stage)

    def retry_job_lease(self, job_name):
        """Re-queues a job on lease execution failure.

        Note:
            This may trigger a job's :class:`Operation` stage transition.

        Args:
            job_name (str): name of the job to retry the lease from.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        try:
            job = self.__jobs_by_name[job_name]

        except KeyError:
            raise NotFoundError("Job name does not exist: [{}]".format(job_name))

        operation_stage = None
        if job.n_tries >= self.MAX_N_TRIES:
            # TODO: Decide what to do with these jobs
            operation_stage = OperationStage.COMPLETED
            # TODO: Mark these jobs as done

        else:
            operation_stage = OperationStage.QUEUED
            self._queue_job(job.name)

            job.update_lease_state(LeaseState.PENDING)

            if self._is_instrumented:
                self.__retries_count += 1

        self._update_job_operation_stage(job_name, operation_stage)

    def get_job_lease(self, job_name):
        """Returns the lease associated to job, if any have been emitted yet.

        Args:
            job_name (str): name of the job to query the lease from.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        try:
            job = self.__jobs_by_name[job_name]

        except KeyError:
            raise NotFoundError("Job name does not exist: [{}]".format(job_name))

        return job.lease

    def delete_job_lease(self, job_name):
        """Discards the lease associated with a job.

        Args:
            job_name (str): name of the job to delete the lease from.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        try:
            job = self.__jobs_by_name[job_name]

        except KeyError:
            raise NotFoundError("Job name does not exist: [{}]".format(job_name))

        job.delete_lease()

        if not job.n_peers and job.done:
            self._delete_job(job.name)

    def get_job_lease_cancelled(self, job_name):
        """Returns true if the lease is cancelled.

        Args:
            job_name (str): name of the job to query the lease state from.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        try:
            job = self.__jobs_by_name[job_name]

        except KeyError:
            raise NotFoundError("Job name does not exist: [{}]".format(job_name))

        return job.lease_cancelled

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
        return len(self.__jobs_by_name)

    def query_n_operations(self):
        # For now n_operations == n_jobs:
        return len(self.__jobs_by_operation)

    def query_n_operations_by_stage(self, operation_stage):
        try:
            if self.__operations_by_stage is not None:
                return len(self.__operations_by_stage[operation_stage])
        except KeyError:
            pass
        return 0

    def query_n_leases(self):
        return len(self.__jobs_by_name)

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

    def _queue_job(self, job_name):
        """Schedules or reschedules a job."""
        job = self.__jobs_by_name[job_name]

        with self.__queue_lock:
            if job.operation_stage == OperationStage.QUEUED:
                self.__queue.sort()

            else:
                bisect.insort(self.__queue, job)

        self.__logger.info("Job queued: [%s]", job.name)

    def _delete_job(self, job_name):
        """Drops an entry from the internal list of jobs."""
        job = self.__jobs_by_name[job_name]

        if job.operation_stage == OperationStage.QUEUED:
            with self.__queue_lock:
                self.__queue.remove(job)

        del self.__jobs_by_action[job.action_digest.hash]
        del self.__jobs_by_name[job.name]

        self.__logger.info("Job deleted: [%s]", job.name)

        if self._is_instrumented:
            self.__operations_by_stage[OperationStage.CACHE_CHECK].discard(job.name)
            self.__operations_by_stage[OperationStage.QUEUED].discard(job.name)
            self.__operations_by_stage[OperationStage.EXECUTING].discard(job.name)
            self.__operations_by_stage[OperationStage.COMPLETED].discard(job.name)

            self.__leases_by_state[LeaseState.PENDING].discard(job.name)
            self.__leases_by_state[LeaseState.ACTIVE].discard(job.name)
            self.__leases_by_state[LeaseState.COMPLETED].discard(job.name)

    def _update_job_operation_stage(self, job_name, operation_stage):
        """Requests a stage transition for the job's :class:Operations.

        Args:
            job_name (str): name of the job to query.
            operation_stage (OperationStage): the stage to transition to.
        """
        job = self.__jobs_by_name[job_name]

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

                if not job.holds_cached_result:
                    execution_metadata = job.action_result.execution_metadata
                    context_metadata = {'job-is': job.name}

                    message = (execution_metadata, context_metadata,)

                    for message_queue in self.__build_metadata_queues:
                        message_queue.put(message)

    def _worker_is_capable(self, worker_capabilities, job):
        """Returns whether the worker is suitable to run the job."""
        # TODO: Replace this with the logic defined in the Platform msg. standard.

        job_requirements = job.platform_requirements
        # For now we'll only check OS and ISA properties.

        if not job_requirements:
            return True

        # OS:
        worker_oses = worker_capabilities.get('os', set())
        job_oses = job_requirements.get('os', set())
        if job_oses and not (job_oses & worker_oses):
            return False

        # ISAs:
        worker_isas = worker_capabilities.get('isa', [])
        job_isas = job_requirements.get('isa', None)

        if job_isas and not (job_isas & worker_isas):
            return False

        return True
