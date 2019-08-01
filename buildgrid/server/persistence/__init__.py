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


from .interface import DataStoreInterface


class DataStore(DataStoreInterface):

    backend = None

    @classmethod
    def activate_monitoring(cls):
        if cls.backend is not None:
            return cls.backend.activate_monitoring()

    @classmethod
    def deactivate_monitoring(cls):
        if cls.backend is not None:
            return cls.backend.deactivate_monitoring()

    @classmethod
    def get_job_by_action(cls, action_digest):
        if cls.backend is not None:
            return cls.backend.get_job_by_action(action_digest)

    @classmethod
    def get_job_by_name(cls, name):
        if cls.backend is not None:
            return cls.backend.get_job_by_name(name)

    @classmethod
    def get_job_by_operation(cls, operation):
        if cls.backend is not None:
            return cls.backend.get_job_by_operation(operation)

    @classmethod
    def get_all_jobs(cls):
        if cls.backend is not None:
            return cls.backend.get_all_jobs()

    @classmethod
    def get_operations_by_stage(cls, operation_stage):
        if cls.backend is not None:
            return cls.backend.get_operations_by_stage(operation_stage)

    @classmethod
    def get_all_operations(cls):
        if cls.backend is not None:
            return cls.backend.get_all_operations()

    @classmethod
    def get_leases_by_state(cls, lease_state):
        if cls.backend is not None:
            return cls.backend.get_leases_by_state(lease_state)

    @classmethod
    def create_job(cls, job):
        if cls.backend is not None:
            cls.backend.create_job(job)

    @classmethod
    def queue_job(cls, job_name):
        if cls.backend is not None:
            cls.backend.queue_job(job_name)

    @classmethod
    def store_response(cls, job):
        if cls.backend is not None:
            cls.backend.store_response(job)

    @classmethod
    def create_operation(cls, operation, job_name):
        if cls.backend is not None:
            cls.backend.create_operation(operation, job_name)

    @classmethod
    def create_lease(cls, lease):
        if cls.backend is not None:
            cls.backend.create_lease(lease)

    @classmethod
    def update_job(cls, job_name, changes):
        if cls.backend is not None:
            cls.backend.update_job(job_name, changes)

    @classmethod
    def update_operation(cls, operation_name, changes):
        if cls.backend is not None:
            cls.backend.update_operation(operation_name, changes)

    @classmethod
    def update_lease(cls, job_name, changes):
        if cls.backend is not None:
            cls.backend.update_lease(job_name, changes)

    @classmethod
    def delete_job(cls, job_name):
        if cls.backend is not None:
            cls.backend.delete_job(job_name)

    @classmethod
    def delete_operation(cls, operation_name):
        if cls.backend is not None:
            cls.backend.delete_operation(operation_name)

    @classmethod
    def load_unfinished_jobs(cls):
        if cls.backend is not None:
            return cls.backend.load_unfinished_jobs()
        return []

    @classmethod
    def assign_lease_for_next_job(cls, capabilities, callback, timeout=None):
        if cls.backend is not None:
            return cls.backend.assign_lease_for_next_job(capabilities, callback, timeout=timeout)
        return None
