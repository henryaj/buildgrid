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

from buildgrid._exceptions import FailedPreconditionError, InvalidArgumentError, NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Action
from buildgrid.utils import get_hash_type


class ExecutionInstance:

    def __init__(self, scheduler, storage):
        self.__logger = logging.getLogger(__name__)

        self._storage = storage
        self._scheduler = scheduler

    @property
    def scheduler(self):
        return self._scheduler

    def register_instance_with_server(self, instance_name, server):
        server.add_execution_instance(self, instance_name)

    def hash_type(self):
        return get_hash_type()

    def execute(self, action_digest, skip_cache_lookup, peer=None, message_queue=None):
        """ Sends a job for execution.
        Queues an action and creates an Operation instance to be associated with
        this action.
        """
        action = self._storage.get_message(action_digest, Action)

        if not action:
            raise FailedPreconditionError("Could not get action from storage.")

        job = self._scheduler.queue_job(action, action_digest,
                                        skip_cache_lookup=skip_cache_lookup)

        if peer is not None and message_queue is not None:
            job.register_operation_peer(peer, message_queue)

        return job.operation

    def register_operation_peer(self, job_name, peer, message_queue):
        try:
            self._scheduler.register_operation_peer(job_name, peer, message_queue)

        except NotFoundError:
            raise InvalidArgumentError("Operation name does not exist: [{}]".format(job_name))

    def unregister_operation_peer(self, job_name, peer):
        try:
            self._scheduler.unregister_operation_peer(job_name, peer)

        except NotFoundError:
            raise InvalidArgumentError("Operation name does not exist: [{}]".format(job_name))

    def stream_operation_updates(self, message_queue, operation_name):
        job = message_queue.get()
        while not job.operation.done:
            yield job.operation
            job = message_queue.get()
            job.check_operation_status()

        yield job.operation
