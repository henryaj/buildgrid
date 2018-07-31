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
ExecutionInstance
=================
An instance of the Remote Execution Server.
"""

import uuid
import logging

from ._exceptions import InvalidArgumentError

from ..job import Job, ExecuteStage

class ExecutionInstance():

    def __init__(self, scheduler):
        self.logger = logging.getLogger(__name__)
        self._scheduler = scheduler

    def execute(self, action_digest, skip_cache_lookup):
        """ Sends a job for execution.
        Queues an action and creates an Operation instance to be associated with
        this action.
        """
        job = Job(action_digest)
        self.logger.info("Operation name: {}".format(job.name))

        if not skip_cache_lookup:
            raise NotImplementedError("ActionCache not implemented")
        else:
            self._scheduler.append_job(job)

        return job.get_operation()

    def get_operation(self, name):
        self.logger.debug("Getting operation: {}".format(name))
        operation = self._scheduler.jobs.get(name)
        if operation is None:
            raise InvalidArgumentError("Operation name does not exist: {}".format(name))
        else:
            return operation.get_operation()

    def list_operations(self, name, list_filter, page_size, page_token):
        # TODO: Pages
        # Spec says number of pages and length of a page are optional
        self.logger.debug("Listing operations")
        return self._scheduler.get_operations()

    def delete_operation(self, name):
        self.logger.debug("Deleting operation {}".format(name))
        try:
            self._scheduler.jobs.pop(name)
        except KeyError:
            raise InvalidArgumentError("Operation name does not exist: {}".format(name))

    def cancel_operation(self, name):
        # TODO: Cancel leases
        raise NotImplementedError("Cancelled operations not supported")
