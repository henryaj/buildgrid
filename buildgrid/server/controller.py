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
Execution Controller
====================

An instance of the Execution controller.

All this stuff you need to make the execution service work.

Contains scheduler, execution instance, an interface to the bots
and an operations instance.
"""


import logging

from .scheduler import Scheduler
from .bots.instance import BotsInterface
from .execution.instance import ExecutionInstance
from .operations.instance import OperationsInstance


class ExecutionController:

    def __init__(self, data_store, *, storage=None, action_cache=None, action_browser_url=None,
                 property_keys=None, bot_session_keepalive_timeout=None):
        self.__logger = logging.getLogger(__name__)

        scheduler = Scheduler(data_store, action_cache=action_cache, action_browser_url=action_browser_url)

        self._execution_instance = ExecutionInstance(scheduler, storage, property_keys)
        self._bots_interface = BotsInterface(scheduler, bot_session_keepalive_timeout=bot_session_keepalive_timeout)
        self._operations_instance = OperationsInstance(scheduler)

    def register_instance_with_server(self, instance_name, server):
        self._execution_instance.register_instance_with_server(instance_name, server)
        self._bots_interface.register_instance_with_server(instance_name, server)
        self._operations_instance.register_instance_with_server(instance_name, server)

    def stream_operation_updates(self, message_queue, operation_name):
        operation = message_queue.get()
        while not operation.done:
            yield operation
            operation = message_queue.get()
        yield operation

    def cancel_operation(self, name):
        # TODO: Cancel leases
        raise NotImplementedError("Cancelled operations not supported")

    @property
    def execution_instance(self):
        return self._execution_instance

    @property
    def bots_interface(self):
        return self._bots_interface

    @property
    def operations_instance(self):
        return self._operations_instance
