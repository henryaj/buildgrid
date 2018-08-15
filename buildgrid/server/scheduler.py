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

from google.protobuf import any_pb2

from buildgrid._protos.google.longrunning import operations_pb2

from .job import ExecuteStage, LeaseState


class Scheduler:

    MAX_N_TRIES = 5

    def __init__(self, action_cache=None):
        self.action_cache = action_cache
        self.jobs = {}
        self.queue = deque()

    def register_client(self, name, queue):
        self.jobs[name].register_client(queue)

    def unregister_client(self, name, queue):
        job = self.jobs[name]
        job.unregister_client(queue)
        if job.check_job_finished():
            del self.jobs[name]

    def append_job(self, job, skip_cache_lookup=False):
        self.jobs[job.name] = job
        if self.action_cache is not None and not skip_cache_lookup:
            cached_result = self.action_cache.get_action_result(job.action_digest)
            if cached_result is not None:
                cached_result_any = any_pb2.Any()
                cached_result_any.Pack(cached_result)
                job.result = cached_result_any
                job.result_cached = True
                job.update_execute_stage(ExecuteStage.COMPLETED)
                return
        self.queue.append(job)
        job.update_execute_stage(ExecuteStage.QUEUED)

    def retry_job(self, name):
        if name in self.jobs:
            job = self.jobs[name]
            if job.n_tries >= self.MAX_N_TRIES:
                # TODO: Decide what to do with these jobs
                job.update_execute_stage(ExecuteStage.COMPLETED)
                # TODO: Mark these jobs as done
            else:
                job.update_execute_stage(ExecuteStage.QUEUED)
                job.n_tries += 1
                self.queue.appendleft(job)

            self.jobs[name] = job

    def job_complete(self, name, result):
        job = self.jobs[name]
        job.result = result
        job.update_execute_stage(ExecuteStage.COMPLETED)
        self.jobs[name] = job
        if not job.do_not_cache and self.action_cache is not None:
            self.action_cache.put_action_result(job.action_digest, result)

    def get_operations(self):
        response = operations_pb2.ListOperationsResponse()
        for v in self.jobs.values():
            response.operations.extend([v.get_operation()])
        return response

    def update_job_lease_state(self, name, state):
        job = self.jobs[name]
        job.lease.state = state
        self.jobs[name] = job

    def get_job_lease(self, name):
        return self.jobs[name].lease

    def cancel_session(self, name):
        job = self.jobs[name]
        state = job.lease.state
        if state in (LeaseState.PENDING.value, LeaseState.ACTIVE.value):
            self.retry_job(name)

    def create_leases(self):
        while self.queue:
            job = self.queue.popleft()
            job.update_execute_stage(ExecuteStage.EXECUTING)
            job.lease = job.create_lease()
            job.lease.state = LeaseState.PENDING.value
            self.jobs[job.name] = job
            yield job.lease
