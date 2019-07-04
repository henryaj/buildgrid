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


from abc import ABC, abstractmethod


class DataStoreInterface(ABC):  # pragma: no cover

    """Abstract class defining an interface to a data storage backend.

    This provides methods for storing the internal state of BuildGrid,
    and retrieving it in order to reconstruct state on restart.

    """

    @abstractmethod
    def activate_monitoring(self):
        raise NotImplementedError()

    @abstractmethod
    def deactivate_monitoring(self):
        raise NotImplementedError()

    @abstractmethod
    def get_job_by_action(self, action_digest):
        raise NotImplementedError()

    @abstractmethod
    def get_job_by_name(self, name):
        raise NotImplementedError()

    @abstractmethod
    def get_job_by_operation(self, operation):
        raise NotImplementedError()

    @abstractmethod
    def get_all_jobs(self):
        raise NotImplementedError()

    @abstractmethod
    def get_operations_by_stage(self, operation_stage):
        raise NotImplementedError()

    @abstractmethod
    def get_all_operations(self):
        raise NotImplementedError()

    @abstractmethod
    def get_leases_by_state(self, lease_state):
        raise NotImplementedError()

    @abstractmethod
    def create_job(self, job):
        raise NotImplementedError()

    @abstractmethod
    def create_operation(self, operation, job_name):
        raise NotImplementedError()

    @abstractmethod
    def create_lease(self, lease):
        raise NotImplementedError()

    @abstractmethod
    def update_job(self, job_name, changes):
        raise NotImplementedError()

    @abstractmethod
    def update_operation(self, operation_name, changes):
        raise NotImplementedError()

    @abstractmethod
    def update_lease(self, job_name, changes):
        raise NotImplementedError()

    @abstractmethod
    def delete_job(self, job_name):
        raise NotImplementedError()

    @abstractmethod
    def delete_operation(self, operation_name):
        raise NotImplementedError()

    @abstractmethod
    def load_unfinished_jobs(self):
        raise NotImplementedError()

    @abstractmethod
    def assign_lease_for_next_job(self, capabilities, callback, timeout=None):
        raise NotImplementedError()
