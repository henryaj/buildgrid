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

import google.devtools.remoteexecution.v1test.remote_execution_pb2

from google.devtools.remoteexecution.v1test.remote_execution_pb2 import ExecuteOperationMetadata
from google.longrunning import operations_pb2_grpc, operations_pb2
from google.protobuf import any_pb2

from .._exceptions import InvalidArgumentError

class ExecutionInstance(object):

    def __init__(self, bots_interface):
        self._bots_interface = bots_interface
        self._operations = {}

    def execute(self, action, skip_cache_lookup):
        """ Sends a job for execution.
        Queues an action and creates an Operation instance to be associated with
        this action.
        """
        operation_meta = ExecuteOperationMetadata()
        operation_name = str(uuid.uuid4())

        if not skip_cache_lookup:
            raise NotImplementedError("ActionCache not implemented")
        else:
            self._enqueue_action(operation_name, action)
            operation_meta.stage = ExecuteOperationMetadata.Stage.Value('QUEUED')
            
        operation = operations_pb2.Operation(name = operation_name,
                                             done = False)
        operation_any = any_pb2.Any()
        operation_any.Pack(operation_meta)
        operation.metadata.CopyFrom(operation_any)

        self._operations[operation_name] = operation        
        return operation

    def get_operation(self, name):
        self._update_operations()
        try:
            return self._operations[name]
        except KeyError:
            raise InvalidArgumentError("Operation name does not exist: {}".format(name))

    def list_operations(self, name, list_filter, page_size, page_token):
        # TODO: Pages
        # Spec says number of pages and length of a page are optional 
        self._update_operations()
        response = operations_pb2.ListOperationsResponse()
        for key, value in self._operations.items():
            response.operations.extend([value])
        return response

    def delete_operation(self, name):
        if name not in self._operations:
            raise InvalidArgumentError("Operation name does not exist: {}".format(name))
        del self._operations[name]

    def cancel_operation(self, name):
        # TODO: Cancel leases
        # Interface currently does not have
        # an implementation for cancelling leases
        raise NotImplementedError

    def _enqueue_action(self, operation_name, action):
        self._bots_interface.enqueue_action(operation_name, action)

    def _update_operations(self):
        """ Gets any actions from the bots_interface which have changed
        state and updates the Operations.
        """
        while not self._bots_interface.operation_queue.empty():
            name, stage = self._bots_interface.operation_queue.get()

            op_any = any_pb2.Any()
            op_meta = ExecuteOperationMetadata()
            op_any.CopyFrom(self._operations[name].metadata)
            op_any.Unpack(op_meta)

            op_meta.stage = ExecuteOperationMetadata.Stage.Value(stage)
            op_any.Pack(op_meta)

            if op_meta.stage == ExecuteOperationMetadata.Stage.Value('COMPLETED'):
                self._operations[name].done = True

            self._operations[name].metadata.CopyFrom(op_any)
