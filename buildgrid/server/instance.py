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
BuildGrid Instance
==================

An instance of the BuildGrid server.

Contains scheduler, execution instance and an interface to the bots.
"""


import logging

from .execution.instance import ExecutionInstance
from .scheduler import Scheduler
from .bots.instance import BotsInterface


class BuildGridInstance(ExecutionInstance, BotsInterface):

    def __init__(self, action_cache=None, cas_storage=None):
        scheduler = Scheduler(action_cache)

        self.logger = logging.getLogger(__name__)

        ExecutionInstance.__init__(self, scheduler, cas_storage)
        BotsInterface.__init__(self, scheduler)

    def stream_operation_updates(self, message_queue, operation_name):
        operation = message_queue.get()
        while not operation.done:
            yield operation
            operation = message_queue.get()
        yield operation

    def cancel_operation(self, name):
        # TODO: Cancel leases
        raise NotImplementedError("Cancelled operations not supported")
