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

from buildgrid._exceptions import NotFoundError

from .job import OperationStage, LeaseState


class Scheduler:

    MAX_N_TRIES = 5

    def __init__(self, action_cache=None):
        self._action_cache = action_cache
        self.jobs = {}
        self.queue = deque()

    def register_client(self, job_name, queue):
        self.jobs[job_name].register_client(queue)

    def unregister_client(self, job_name, queue):
        self.jobs[job_name].unregister_client(queue)

        if not self.jobs[job_name].n_clients and self.jobs[job_name].operation.done:
            del self.jobs[job_name]

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

        else:
            operation_stage = OperationStage.QUEUED
            self.queue.append(job)

        job.update_operation_stage(operation_stage)

    def retry_job(self, job_name):
        if job_name in self.jobs:
            job = self.jobs[job_name]
            if job.n_tries >= self.MAX_N_TRIES:
                # TODO: Decide what to do with these jobs
                job.update_operation_stage(OperationStage.COMPLETED)
                # TODO: Mark these jobs as done
            else:
                job.update_operation_stage(OperationStage.QUEUED)
                self.queue.appendleft(job)

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
        # For now, one lease at a time:
        lease = job.create_lease()

        return [lease]

    def update_job_lease_state(self, job_name, lease_state, lease_status=None, lease_result=None):
        """Requests a state transition for a job's current :class:Lease.

        Args:
            job_name (str): name of the job to query.
            lease_state (LeaseState): the lease state to transition to.
            lease_status (google.rpc.Status): the lease execution status, only
                required if `lease_state` is `COMPLETED`.
            lease_result (google.protobuf.Any): the lease execution result, only
                required if `lease_state` is `COMPLETED`.
        """
        job = self.jobs[job_name]

        if lease_state != LeaseState.COMPLETED:
            job.update_lease_state(lease_state)

        else:
            job.update_lease_state(lease_state,
                                   status=lease_status, result=lease_result)

            if not job.do_not_cache and self._action_cache is not None:
                if not job.lease.status.code:
                    self._action_cache.update_action_result(job.action_digest, job.action_result)

            job.update_operation_stage(OperationStage.COMPLETED)

    def get_job_lease(self, job_name):
        """Returns the lease associated to job, if any have been emitted yet."""
        return self.jobs[job_name].lease

    def get_job_operation(self, job_name):
        """Returns the operation associated to job."""
        return self.jobs[job_name].operation
