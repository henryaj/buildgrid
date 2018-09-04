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

from google.protobuf.empty_pb2 import Empty

from buildgrid._protos.google.longrunning import operations_pb2_grpc, operations_pb2

from .._exceptions import InvalidArgumentError


class OperationsService(operations_pb2_grpc.OperationsServicer):

    def __init__(self, server, instances):
        self._instances = instances
        self.logger = logging.getLogger(__name__)

        operations_pb2_grpc.add_OperationsServicer_to_server(self, server)

    def GetOperation(self, request, context):
        try:
            name = request.name
            operation_name = self._get_operation_name(name)

            instance = self._get_instance(name)

            operation = instance.get_operation(operation_name)
            operation.name = name
            return operation

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return operations_pb2.Operation()

    def ListOperations(self, request, context):
        try:
            # Name should be the collection name
            # Or in this case, the instance_name
            name = request.name
            instance = self._get_instance(name)

            result = instance.list_operations(request.filter,
                                              request.page_size,
                                              request.page_token)

            for operation in result.operations:
                operation.name = "{}/{}".format(name, operation.name)

            return result

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return operations_pb2.ListOperationsResponse()

    def DeleteOperation(self, request, context):
        try:
            name = request.name
            operation_name = self._get_operation_name(name)

            instance = self._get_instance(name)

            instance.delete_operation(operation_name)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return Empty()

    def CancelOperation(self, request, context):
        try:
            name = request.name
            operation_name = self._get_operation_name(name)

            instance = self._get_instance(name)

            instance.cancel_operation(operation_name)

        except NotImplementedError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return Empty()

    def _get_operation_name(self, name):
        return name.split("/")[-1]

    def _get_instance(self, name):
        try:
            names = name.split("/")

            # Operation name should be in format:
            # {instance/name}/{operation_id}
            instance_name = ''.join(names[0:-1])
            if not instance_name:
                return self._instances[name]

            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError("Instance doesn't exist on server: {}".format(name))
