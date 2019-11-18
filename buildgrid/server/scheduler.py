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

    def __init__(self, data_store, action_cache=None, action_browser_url=False, monitor=False):
        self.__logger = logging.getLogger(__name__)

        self._instance_name = None

        self.__build_metadata_queues = None

        self.__queue_time_average = None
        self.__retries_count = 0

        self._action_cache = action_cache
        self._action_browser_url = action_browser_url

        self.__operation_lock = Lock()  # Lock protecting deletion, addition  and updating of jobs

        self.__operations_by_peer = {}
        self.__peer_message_queues = {}

        self.data_store = data_store

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
        jobs = self.data_store.get_all_jobs()
        return [job.name for job in jobs]

    def list_job_operations(self, job_name):
        """Returns a list of :class:`Operation` names for a :class:`Job`."""
        job = self.data_store.get_job_by_name(job_name)
        if job is not None:
            return job.list_operations()
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
        with self.__operation_lock:
            job = self.data_store.get_job_by_name(job_name)

            if job is None:
                raise NotFoundError("Job name does not exist: [{}]".format(job_name))

            operation_name = job.register_new_operation_peer(
                peer, message_queue, self.__operations_by_peer, self.__peer_message_queues,
                data_store=self.data_store)

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
        with self.__operation_lock:
            job = self.data_store.get_job_by_operation(operation_name)

            if job is None:
                raise NotFoundError("Operation name does not exist: [{}]"
                                    .format(operation_name))

            job.register_operation_peer(operation_name,
                                        peer,
                                        message_queue,
                                        self.__operations_by_peer,
                                        self.__peer_message_queues,
                                        data_store=self.data_store)

    def unregister_job_operation_peer(self, operation_name, peer):
        """Unsubscribes to one of the job's :class:`Operation` stage change.

        Args:
            operation_name (str): name of the operation to unsubscribe from.
            peer (str): a unique string identifying the client.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        with self.__operation_lock:
            job = self.data_store.get_job_by_operation(operation_name)

            if job is None:
                raise NotFoundError("Operation name does not exist: [{}]"
                                    .format(operation_name))

            job.unregister_operation_peer(operation_name, peer)
            self.__operations_by_peer[peer].remove(operation_name)
            self.__peer_message_queues[peer].pop(operation_name)

            if not job.n_peers_for_operation(operation_name, self.__operations_by_peer):
                self.data_store.delete_operation(operation_name)

            if not job.n_peers(self.__operations_by_peer) and job.done and not job.lease:
                self.data_store.delete_job(job.name)

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
        job = self.data_store.get_job_by_action(action_digest)
        if job is not None and not action.do_not_cache:
            # If existing job has been cancelled or isn't
            # cacheable, create a new one.
            if not job.cancelled and not job.do_not_cache:
                # Reschedule if priority is now greater:
                if priority < job.priority:
                    job.set_priority(priority, data_store=self.data_store)

                    if job.operation_stage == OperationStage.QUEUED:
                        self.data_store.queue_job(job.name)

                self.__logger.debug("Job deduplicated for action [%s]: [%s]",
                                    action_digest.hash[:8], job.name)

                return job.name

        job = Job(action.do_not_cache, action_digest,
                  platform_requirements=platform_requirements,
                  priority=priority)
        self.data_store.create_job(job)

        if self._action_browser_url:
            job.set_action_url(
                BrowserURL(self._action_browser_url, self._instance_name))

        self.__logger.debug("Job created for action [%s]: [%s]",
                            action_digest.hash[:8], job.name)

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
                self.data_store.queue_job(job.name)

        else:
            operation_stage = OperationStage.QUEUED
            self.data_store.queue_job(job.name)

        self._update_job_operation_stage(job.name, operation_stage)

        return job.name

    def get_job_operation(self, operation_name):
        """Retrieves a job's :class:`Operation` by name.

        Args:
            operation_name (str): name of the operation to query.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        job = self.data_store.get_job_by_operation(operation_name)

        if job is None:
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
        job = self.data_store.get_job_by_operation(operation_name)

        if job is None:
            raise NotFoundError("Operation name does not exist: [{}]"
                                .format(operation_name))

        job.cancel_operation(
            operation_name, self.__operations_by_peer,
            self.__peer_message_queues, data_store=self.data_store)

    def delete_job_operation(self, operation_name):
        """"Removes a job.

        Args:
            operation_name (str): name of the operation to delete.

        Raises:
            NotFoundError: If no operation with `operation_name` exists.
        """
        with self.__operation_lock:
            job = self.data_store.get_job_by_operation(operation_name)

            if job is None:
                raise NotFoundError("Operation name does not exist: [{}]"
                                    .format(operation_name))
            if not job.n_peers(self.__operations_by_peer) and job.done and not job.lease:
                self.data_store.delete_job(job.name)

    # --- Public API: RWAPI ---

    def request_job_leases(self, worker_capabilities, timeout=None, worker_name=None, bot_id=None):
        """Generates a list of the highest priority leases to be run.

        Args:
            worker_capabilities (dict): a set of key-value pairs describing the
                worker properties, configuration and state at the time of the
                request.
            timeout (int): time to block waiting on job queue, caps if longer
                than MAX_JOB_BLOCK_TIME
            worker_name (string): name of the worker requesting the leases.
        """
        def assign_lease(job):
            self.__logger.info("Job scheduled to run: [%s]", job.name)

            lease = job.lease

            if not lease:
                # For now, one lease at a time:
                lease = job.create_lease(worker_name, bot_id, data_store=self.data_store)

            if lease:
                job.mark_worker_started()
                return [lease]
            return []

        leases = self.data_store.assign_lease_for_next_job(
            worker_capabilities, assign_lease, timeout=timeout)
        if leases:
            # Update the leases outside of the callback to avoid nested data_store operations
            for lease in leases:
                # The lease id and job names are the same, so use that as the job name
                self._update_job_operation_stage(lease.id, OperationStage.EXECUTING)
        return leases

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
        job = self.data_store.get_job_by_name(job_name)

        if job is None:
            raise NotFoundError("Job name does not exist: [{}]".format(job_name))

        lease_state = LeaseState(lease.state)

        operation_stage = None
        if lease_state == LeaseState.PENDING:
            job.update_lease_state(LeaseState.PENDING, data_store=self.data_store)
            operation_stage = OperationStage.QUEUED

        elif lease_state == LeaseState.ACTIVE:
            job.update_lease_state(LeaseState.ACTIVE, data_store=self.data_store)
            operation_stage = OperationStage.EXECUTING

        elif lease_state == LeaseState.COMPLETED:
            job.update_lease_state(LeaseState.COMPLETED,
                                   status=lease.status, result=lease.result,
                                   data_store=self.data_store)

            if (self._action_cache is not None and
                    self._action_cache.allow_updates and not job.do_not_cache):
                self._action_cache.update_action_result(job.action_digest, job.action_result)
            self.data_store.store_response(job)

            operation_stage = OperationStage.COMPLETED

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
        job = self.data_store.get_job_by_name(job_name)

        if job is None:
            raise NotFoundError("Job name does not exist: [{}]".format(job_name))

        operation_stage = None
        if job.n_tries >= self.MAX_N_TRIES:
            # TODO: Decide what to do with these jobs
            operation_stage = OperationStage.COMPLETED
            # TODO: Mark these jobs as done

        else:
            operation_stage = OperationStage.QUEUED
            self.data_store.queue_job(job.name)

            job.update_lease_state(LeaseState.PENDING, data_store=self.data_store)

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
        job = self.data_store.get_job_by_name(job_name)

        if job is None:
            raise NotFoundError("Job name does not exist: [{}]".format(job_name))

        return job.lease

    def delete_job_lease(self, job_name):
        """Discards the lease associated with a job.

        Args:
            job_name (str): name of the job to delete the lease from.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        with self.__operation_lock:
            job = self.data_store.get_job_by_name(job_name)

            if job is None:
                raise NotFoundError("Job name does not exist: [{}]".format(job_name))

            job.delete_lease()

            if not job.n_peers(self.__operations_by_peer) and job.done:
                self.data_store.delete_job(job.name)

    def get_job_lease_cancelled(self, job_name):
        """Returns true if the lease is cancelled.

        Args:
            job_name (str): name of the job to query the lease state from.

        Raises:
            NotFoundError: If no job with `job_name` exists.
        """
        job = self.data_store.get_job_by_name(job_name)

        if job is None:
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

        self.__queue_time_average = 0, timedelta()
        self.__retries_count = 0

        self._is_instrumented = True

    def deactivate_monitoring(self):
        """Deactivated jobs monitoring."""
        if not self._is_instrumented:
            return

        self._is_instrumented = False

        self.__build_metadata_queues = None

        self.__queue_time_average = None
        self.__retries_count = 0

    def register_build_metadata_watcher(self, message_queue):
        if self.__build_metadata_queues is not None:
            self.__build_metadata_queues.append(message_queue)

    def query_n_jobs(self):
        return len(self.data_store.get_all_jobs())

    def query_n_operations(self):
        # For now n_operations == n_jobs:
        return len(self.data_store.get_all_operations())

    def query_n_operations_by_stage(self, operation_stage):
        return len(self.data_store.get_operations_by_stage(operation_stage))

    def query_n_leases(self):
        return len(self.data_store.get_all_jobs())

    def query_n_leases_by_state(self, lease_state):
        return len(self.data_store.get_leases_by_state(lease_state))

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
        with self.__operation_lock:
            job = self.data_store.get_job_by_name(job_name)

            if operation_stage == OperationStage.CACHE_CHECK:
                job.update_operation_stage(OperationStage.CACHE_CHECK,
                                           self.__operations_by_peer,
                                           self.__peer_message_queues,
                                           data_store=self.data_store)

            elif operation_stage == OperationStage.QUEUED:
                job.update_operation_stage(OperationStage.QUEUED,
                                           self.__operations_by_peer,
                                           self.__peer_message_queues,
                                           data_store=self.data_store)

            elif operation_stage == OperationStage.EXECUTING:
                job.update_operation_stage(OperationStage.EXECUTING,
                                           self.__operations_by_peer,
                                           self.__peer_message_queues,
                                           data_store=self.data_store)

            elif operation_stage == OperationStage.COMPLETED:
                job.update_operation_stage(OperationStage.COMPLETED,
                                           self.__operations_by_peer,
                                           self.__peer_message_queues,
                                           data_store=self.data_store)

                if self._is_instrumented:
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
