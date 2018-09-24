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


from buildgrid.server._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.longrunning import operations_pb2

from .job import ExecuteStage, LeaseState


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
        if job.check_job_finished():
            del self.jobs[name]

    def append_job(self, job, skip_cache_lookup=False):
        self.jobs[job.name] = job
        if self._action_cache is not None and not skip_cache_lookup:
            try:
                cached_result = self._action_cache.get_action_result(job.action_digest)
            except NotFoundError:
                self.queue.append(job)
                job.update_execute_stage(ExecuteStage.QUEUED)

            else:
                cached_result_any = any_pb2.Any()
                cached_result_any.Pack(cached_result)
                job.result = cached_result_any
                job.result_cached = True
                job.update_execute_stage(ExecuteStage.COMPLETED)

        else:
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

    def job_complete(self, name, result, status):
        job = self.jobs[name]
        job.lease.status.CopyFrom(status)
        action_result = remote_execution_pb2.ActionResult()
        result.Unpack(action_result)
        job.result = action_result
        if not job.do_not_cache and self._action_cache is not None:
            if not job.lease.status.code:
                self._action_cache.update_action_result(job.action_digest, action_result)
        job.update_execute_stage(ExecuteStage.COMPLETED)

    def get_operations(self):
        response = operations_pb2.ListOperationsResponse()
        for v in self.jobs.values():
            response.operations.extend([v.get_operation()])
        return response

    def update_job_lease_state(self, name, state):
        job = self.jobs[name]
        job.lease.state = state

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
            job.create_lease()
            job.lease.state = LeaseState.PENDING.value
            yield job.lease
