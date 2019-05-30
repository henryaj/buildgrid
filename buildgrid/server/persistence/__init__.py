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
    def create_job(cls, job):
        if cls.backend is not None:
            cls.backend.create_job(job)

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
    def load_unfinished_jobs(cls):
        if cls.backend is not None:
            return cls.backend.load_unfinished_jobs()
        return []

    @classmethod
    def get_next_runnable_job(cls, capabilities):
        if cls.backend is not None:
            return cls.backend.get_next_runnable_job(capabilities)
        return None
