# Copyright (C) 2019 Bloomberg LP
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


import bisect
import logging
from threading import Lock
import time

from ...._enums import LeaseState, OperationStage
from ....utils import Condition
from ....settings import MAX_JOB_BLOCK_TIME
from ..interface import DataStoreInterface


class MemoryDataStore(DataStoreInterface):

    def __init__(self, storage):
        self.logger = logging.getLogger(__file__)
        self.logger.info("Creating in-memory data store interface")

        self.queue = []
        self.queue_lock = Lock()
        self.queue_condition = Condition(lock=self.queue_lock)

        self.jobs_by_action = {}
        self.jobs_by_operation = {}
        self.jobs_by_name = {}

        self.operations_by_stage = {}
        self.leases_by_state = {}
        self.is_instrumented = False

    def __repr__(self):
        return "In-memory data store interface"

    def activate_monitoring(self):
        if self.is_instrumented:
            return

        self.operations_by_stage = {
            stage: set() for stage in OperationStage
        }
        self.leases_by_state = {
            state: set() for state in LeaseState
        }
        self.is_instrumented = True

    def deactivate_monitoring(self):
        if not self.is_instrumented:
            return

        self.operations_by_stage = {}
        self.leases_by_state = {}
        self.is_instrumented = False

    def get_job_by_name(self, name):
        return self.jobs_by_name.get(name)

    def get_job_by_action(self, action_digest):
        return self.jobs_by_action.get(action_digest.hash)

    def get_job_by_operation(self, name):
        return self.jobs_by_operation.get(name)

    def get_all_jobs(self):
        return [job for job in self.jobs_by_name.values()
                if job.operation_stage != OperationStage.COMPLETED]

    def create_job(self, job):
        self.jobs_by_action[job.action_digest.hash] = job
        self.jobs_by_name[job.name] = job

    def queue_job(self, job_name):
        job = self.jobs_by_name[job_name]
        with self.queue_condition:
            if job.operation_stage != OperationStage.QUEUED:
                bisect.insort(self.queue, job)
                self.logger.info("Job queued: [%s]", job.name)
            else:
                self.logger.info("Job already queued: [%s]", job.name)
                self.queue.sort()

    def update_job(self, job_name, changes):
        # With this implementation, there's no need to actually make
        # changes to the stored job, since its a reference to the
        # in-memory job that caused this method to be called.
        pass

    def delete_job(self, job_name):
        job = self.jobs_by_name[job_name]

        del self.jobs_by_action[job.action_digest.hash]
        del self.jobs_by_name[job.name]

        self.logger.info("Job deleted: [%s]", job.name)

        if self.is_instrumented:
            for stage in OperationStage:
                self.operations_by_stage[stage].discard(job.name)

            for state in LeaseState:
                self.leases_by_state[state].discard(job.name)

    def store_response(self, job):
        # The job is always in memory in this implementation, so there's
        # no need to write anything to the CAS, since the job stays in
        # memory as long as we need it
        pass

    def get_operations_by_stage(self, operation_stage):
        return self.operations_by_stage.get(operation_stage, set())

    def get_all_operations(self):
        return self.jobs_by_operation.keys()

    def create_operation(self, operation, job_name):
        job = self.jobs_by_name[job_name]
        self.jobs_by_operation[operation.name] = job
        if self.is_instrumented:
            self.operations_by_stage[job.operation_stage].add(job_name)

    def update_operation(self, operation_name, changes):
        if self.is_instrumented:
            job = self.jobs_by_operation[operation_name]
            self.operations_by_stage[job.operation_stage].add(job.name)
            other_stages = [member for member in OperationStage if member != job.operation_stage]
            for stage in other_stages:
                self.operations_by_stage[stage].discard(job.name)

    def delete_operation(self, operation_name):
        del self.jobs_by_operation[operation_name]

    def get_leases_by_state(self, lease_state):
        return self.leases_by_state.get(lease_state, set())

    def create_lease(self, lease):
        if self.is_instrumented:
            self.leases_by_state[LeaseState(lease.state)].add(lease.id)

    def update_lease(self, job_name, changes):
        if self.is_instrumented:
            job = self.jobs_by_name[job_name]
            state = LeaseState(job.lease.state)
            self.leases_by_state[state].add(job.lease.id)
            other_states = [member for member in LeaseState if member != state]
            for state in other_states:
                self.leases_by_state[state].discard(job.lease.id)

    def load_unfinished_jobs(self):
        return []

    def assign_lease_for_next_job(self, capabilities, callback, timeout=None):
        """Return the highest priority job that can be run by a worker.

        Iterate over the job queue and find the highest priority job which
        the worker can run given the provided capabilities. Takes a
        dictionary of worker capabilities to compare with job requirements.

        :param capabilities: Dictionary of worker capabilities to compare
            with job requirements when finding a job.
        :type capabilities: dict
        :param callback: Function to run on the next runnable job, should return
            a list of leases.
        :type callback: function
        :param timeout: time to block waiting on job queue, caps if longer
            than MAX_JOB_BLOCK_TIME.
        :type timeout: int
        :returns: A job

        """
        if not timeout and not self.queue:
            return []

        with self.queue_condition:
            leases = self._assign_lease(capabilities, callback)

            self.queue_condition.notify()

            if timeout:
                # Cap the timeout if it's larger than MAX_JOB_BLOCK_TIME
                timeout = min(timeout, MAX_JOB_BLOCK_TIME)
                deadline = time.time() + timeout
                while not leases and time.time() < deadline:
                    ready = self.queue_condition.wait(timeout=deadline - time.time())
                    if not ready:
                        # If we ran out of time waiting for the condition variable,
                        # give up early.
                        break
                    leases = self._assign_lease(capabilities, callback, deadline=deadline)
                    self.queue_condition.notify()

        return leases

    def _assign_lease(self, worker_capabilities, callback, deadline=None):
        for index, job in enumerate(self.queue):
            if deadline is not None and time.time() >= deadline:
                break
            # Don't queue a cancelled job, it would be unable to get a lease anyway
            if job.cancelled:
                self.logger.debug("Dropping cancelled job: [%s] from queue", job.name)
                del self.queue[index]
                continue

            if self._worker_is_capable(worker_capabilities, job):
                leases = callback(job)
                if leases:
                    del self.queue[index]
                    return leases
        return []

    def _worker_is_capable(self, worker_capabilities, job):
        """Returns whether the worker is suitable to run the job."""
        # TODO: Replace this with the logic defined in the Platform msg. standard.

        job_requirements = job.platform_requirements
        # For now we'll only check OS and ISA properties.

        if not job_requirements:
            return True

        for req, matches in job_requirements.items():
            if not matches <= worker_capabilities.get(req, set()):
                return False
        return True
