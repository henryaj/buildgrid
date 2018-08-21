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
OperationsService
===============

"""

import logging

import grpc

from buildgrid._protos.google.longrunning import operations_pb2_grpc, operations_pb2

from .._exceptions import InvalidArgumentError


class OperationsService(operations_pb2_grpc.OperationsServicer):

    def __init__(self, instance):
        self._instance = instance
        self.logger = logging.getLogger(__name__)

    def GetOperation(self, request, context):
        try:
            return self._instance.get_operation(request.name)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return operations_pb2.Operation()

    def ListOperations(self, request, context):
        return self._instance.list_operations(request.name,
                                              request.filter,
                                              request.page_size,
                                              request.page_token)

    def DeleteOperation(self, request, context):
        try:
            return self._instance.delete_operation(request.name)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return operations_pb2.Operation()

    def CancelOperation(self, request, context):
        try:
            return self._instance.cancel_operation(request.name)

        except NotImplementedError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            return operations_pb2.Operation()
