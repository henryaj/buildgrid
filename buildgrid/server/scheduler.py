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

from google.longrunning import operations_pb2

from .job import ExecuteStage, LeaseState

class Scheduler():

    MAX_N_TRIES = 5

    def __init__(self):
        self.jobs = {}
        self.queue = deque()

    def append_job(self, job):
        job.execute_stage = ExecuteStage.QUEUED
        self.jobs[job.name] = job
        self.queue.append(job)

    def retry_job(self, name):
        job = self.jobs[name]

        if job.n_tries >= self.MAX_N_TRIES:
            job.execute_stage = ExecuteStage.COMPLETED
            job.cancel()
        else:
            job.execute_stage = ExecuteStage.QUEUED
            job.n_tries += 1
            self.queue.appendleft(job)

        self.jobs[name] = job

    def get_job(self):
        job = self.queue.popleft()
        job.execute_stage = ExecuteStage.EXECUTING
        job.lease_state   = LeaseState.PENDING
        self.jobs[job.name] = job
        return job

    def job_complete(self, name, result):
        job = self.jobs[name]
        job.execute_stage = ExecuteStage.COMPLETED
        job.result = result
        self.jobs[name] = job

    def get_operations(self):
        response = operations_pb2.ListOperationsResponse()
        for v in self.jobs.values():
            response.operations.extend([v.get_operation()])
        return response
