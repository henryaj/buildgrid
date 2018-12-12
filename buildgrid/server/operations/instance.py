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
OperationsInstance
=================
An instance of the LongRunningOperations Service.
"""

import logging

from buildgrid._exceptions import InvalidArgumentError, NotFoundError
from buildgrid._protos.google.longrunning import operations_pb2


class OperationsInstance:

    def __init__(self, scheduler):
        self.__logger = logging.getLogger(__name__)

        self._scheduler = scheduler

    @property
    def scheduler(self):
        return self._scheduler

    def register_instance_with_server(self, instance_name, server):
        server.add_operations_instance(self, instance_name)

    def get_operation(self, job_name):
        try:
            operation = self._scheduler.get_job_operation(job_name)

        except NotFoundError:
            raise InvalidArgumentError("Operation name does not exist: [{}]".format(job_name))

        return operation

    def list_operations(self, list_filter, page_size, page_token):
        # TODO: Pages
        # Spec says number of pages and length of a page are optional
        response = operations_pb2.ListOperationsResponse()

        operation_names = [operation_name for job_name in
                           self._scheduler.list_current_jobs() for operation_name in
                           self._scheduler.list_job_operations(job_name)]

        operations = []
        for operation_name in operation_names:
            operation = self._scheduler.get_job_operation(operation_name)
            operations.append(operation)

        response.operations.extend(operations)

        return response

    def delete_operation(self, job_name):
        try:
            self._scheduler.delete_job_operation(job_name)

        except NotFoundError:
            raise InvalidArgumentError("Operation name does not exist: [{}]".format(job_name))

    def cancel_operation(self, job_name):
        try:
            self._scheduler.cancel_job_operation(job_name)

        except NotFoundError:
            raise InvalidArgumentError("Operation name does not exist: [{}]".format(job_name))
