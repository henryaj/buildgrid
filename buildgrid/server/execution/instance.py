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
ExecutionInstance
=================
An instance of the Remote Execution Service.
"""

import logging

from buildgrid._exceptions import FailedPreconditionError, InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Action

from ..job import Job


class ExecutionInstance:

    def __init__(self, scheduler, storage):
        self.logger = logging.getLogger(__name__)
        self._storage = storage
        self._scheduler = scheduler

    def register_instance_with_server(self, instance_name, server):
        server.add_execution_instance(self, instance_name)

    def execute(self, action_digest, skip_cache_lookup, message_queue=None):
        """ Sends a job for execution.
        Queues an action and creates an Operation instance to be associated with
        this action.
        """

        action = self._storage.get_message(action_digest, Action)

        if not action:
            raise FailedPreconditionError("Could not get action from storage.")

        job = Job(action, action_digest)
        if message_queue is not None:
            job.register_client(message_queue)

        self.logger.info("Operation name: [{}]".format(job.name))

        self._scheduler.queue_job(job, skip_cache_lookup)

        return job.operation

    def register_message_client(self, name, queue):
        try:
            self._scheduler.register_client(name, queue)

        except KeyError:
            raise InvalidArgumentError("Operation name does not exist: [{}]".format(name))

    def unregister_message_client(self, name, queue):
        try:
            self._scheduler.unregister_client(name, queue)

        except KeyError:
            raise InvalidArgumentError("Operation name does not exist: [{}]".format(name))

    def stream_operation_updates(self, message_queue, operation_name):
        operation = message_queue.get()
        while not operation.done:
            yield operation
            operation = message_queue.get()
        yield operation
