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
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Action, Command
from buildgrid.utils import get_hash_type


class ExecutionInstance:

    def __init__(self, scheduler, storage, property_keys):
        self.__logger = logging.getLogger(__name__)

        self._scheduler = scheduler
        self._instance_name = None
        self._standard_keys = set(["ISA", "OSFamily"])
        self._property_keys = set(property_keys) if property_keys else set()
        self.__storage = storage

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @property
    def scheduler(self):
        return self._scheduler

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the execution instance with a given server."""
        if self._instance_name is None:
            server.add_execution_instance(self, instance_name)

            self._instance_name = instance_name
            if self._scheduler is not None:
                self._scheduler.set_instance_name(instance_name)

        else:
            raise AssertionError("Instance already registered")

    def hash_type(self):
        return get_hash_type()

    def execute(self, action_digest, skip_cache_lookup):
        """ Sends a job for execution.
        Queues an action and creates an Operation instance to be associated with
        this action.
        """
        action = self.__storage.get_message(action_digest, Action)

        if not action:
            raise FailedPreconditionError("Could not get action from storage.")

        command = self.__storage.get_message(action.command_digest, Command)

        if not command:
            raise FailedPreconditionError(
                "Could not get command from storage.")

        platform_requirements = {}
        for platform_property in command.platform.properties:
            if platform_property.name not in self._standard_keys:
                if platform_property.name not in self._property_keys:
                    raise FailedPreconditionError(
                        "Unregistered platform property.")

            elif platform_property.name == "OSFamily":
                if platform_property.name in platform_requirements:
                    raise FailedPreconditionError(
                        "Multiple OSFamilies specified.")

            if platform_property.name not in platform_requirements:
                platform_requirements[platform_property.name] = set()
            platform_requirements[platform_property.name].add(
                platform_property.value)

        return self._scheduler.queue_job_action(action, action_digest,
                                                platform_requirements=platform_requirements,
                                                skip_cache_lookup=skip_cache_lookup)

    def register_job_peer(self, job_name, peer, message_queue):
        try:
            return self._scheduler.register_job_peer(job_name,
                                                     peer, message_queue)

        except NotFoundError:
            raise InvalidArgumentError("Job name does not exist: [{}]"
                                       .format(job_name))

    def register_operation_peer(self, operation_name, peer, message_queue):
        try:
            self._scheduler.register_job_operation_peer(operation_name,
                                                        peer, message_queue)

        except NotFoundError:
            raise InvalidArgumentError("Operation name does not exist: [{}]"
                                       .format(operation_name))

    def unregister_operation_peer(self, operation_name, peer):
        try:
            self._scheduler.unregister_job_operation_peer(operation_name, peer)

        except NotFoundError:
            # Operation already unregistered due to being finished/cancelled
            pass

    def stream_operation_updates(self, message_queue):
        error, operation = message_queue.get()
        if error is not None:
            raise error

        while not operation.done:
            yield operation

            error, operation = message_queue.get()
            if error is not None:
                error.last_response = operation
                raise error

        yield operation
