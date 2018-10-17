# Copyright (C) 2018 Codethink Limited
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
#
# Authors:
#        Finn Ball <finn.ball@codethink.co.uk>

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

    def register_client(self, name, queue):
        self.jobs[name].register_client(queue)

    def unregister_client(self, name, queue):
        job = self.jobs[name]
        job.unregister_client(queue)
        if job.operation.done:
            del self.jobs[name]

    def append_job(self, job, skip_cache_lookup=False):
        self.jobs[job.name] = job
        if self._action_cache is not None and not skip_cache_lookup:
            try:
                cached_result = self._action_cache.get_action_result(job.action_digest)
            except NotFoundError:
                self.queue.append(job)
                job.update_operation_stage(OperationStage.QUEUED)

            else:
                job.set_cached_result(cached_result)
                job.update_operation_stage(OperationStage.COMPLETED)

        else:
            self.queue.append(job)
            job.update_operation_stage(OperationStage.QUEUED)

    def retry_job(self, name):
        if name in self.jobs:
            job = self.jobs[name]
            if job.n_tries >= self.MAX_N_TRIES:
                # TODO: Decide what to do with these jobs
                job.update_operation_stage(OperationStage.COMPLETED)
                # TODO: Mark these jobs as done
            else:
                job.update_operation_stage(OperationStage.QUEUED)
                self.queue.appendleft(job)

    def list_jobs(self):
        return self.jobs.values()

    def update_job_lease_state(self, job_name, lease_state, lease_status=None, lease_result=None):
        job = self.jobs[job_name]
        if lease_state != LeaseState.COMPLETED:
            job.update_lease_state(lease_state)
        else:
            job.update_lease_state(lease_state, status=lease_status, result=lease_result)

            if not job.do_not_cache and self._action_cache is not None:
                if not job.lease.status.code:
                    self._action_cache.update_action_result(job.action_digest, job.action_result)

            job.update_operation_stage(OperationStage.COMPLETED)

    def get_job_lease(self, name):
        return self.jobs[name].lease

    def cancel_session(self, name):
        job = self.jobs[name]
        state = job.lease.state
        if state in (LeaseState.PENDING.value, LeaseState.ACTIVE.value):
            self.retry_job(name)

    def create_lease(self):
        if self.queue:
            job = self.queue.popleft()
            job.update_operation_stage(OperationStage.EXECUTING)
            job.create_lease()
            return job.lease
        return None
