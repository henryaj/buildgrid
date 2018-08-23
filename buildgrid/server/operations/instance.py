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

from .._exceptions import InvalidArgumentError


class OperationsInstance:

    def __init__(self, scheduler):
        self.logger = logging.getLogger(__name__)
        self._scheduler = scheduler

    def get_operation(self, name):
        operation = self._scheduler.jobs.get(name)

        if operation is None:
            raise InvalidArgumentError("Operation name does not exist: {}".format(name))

        else:
            return operation.get_operation()

    def list_operations(self, list_filter, page_size, page_token):
        # TODO: Pages
        # Spec says number of pages and length of a page are optional
        return self._scheduler.get_operations()

    def delete_operation(self, name):
        try:
            self._scheduler.jobs.pop(name)

        except KeyError:
            raise InvalidArgumentError("Operation name does not exist: {}".format(name))

    def register_message_client(self, name, queue):
        try:
            self._scheduler.register_client(name, queue)

        except KeyError:
            raise InvalidArgumentError("Operation name does not exist: {}".format(name))

    def unregister_message_client(self, name, queue):
        try:
            self._scheduler.unregister_client(name, queue)

        except KeyError:
            raise InvalidArgumentError("Operation name does not exist: {}".format(name))
