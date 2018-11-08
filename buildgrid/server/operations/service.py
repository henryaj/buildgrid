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

from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.google.longrunning import operations_pb2_grpc, operations_pb2


class OperationsService(operations_pb2_grpc.OperationsServicer):

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        operations_pb2_grpc.add_OperationsServicer_to_server(self, server)

    def add_instance(self, name, instance):
        self._instances[name] = instance

    def GetOperation(self, request, context):
        self.__logger.debug("GetOperation request from [%s]", context.peer())

        try:
            name = request.name

            instance_name = self._parse_instance_name(name)
            instance = self._get_instance(instance_name)

            operation_name = self._parse_operation_name(name)
            operation = instance.get_operation(operation_name)
            op = operations_pb2.Operation()
            op.CopyFrom(operation)
            op.name = name
            return op

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return operations_pb2.Operation()

    def ListOperations(self, request, context):
        self.__logger.debug("ListOperations request from [%s]", context.peer())

        try:
            # The request name should be the collection name
            # In our case, this is just the instance_name
            instance_name = request.name
            instance = self._get_instance(instance_name)

            result = instance.list_operations(request.filter,
                                              request.page_size,
                                              request.page_token)

            for operation in result.operations:
                operation.name = "{}/{}".format(instance_name, operation.name)

            return result

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return operations_pb2.ListOperationsResponse()

    def DeleteOperation(self, request, context):
        self.__logger.debug("DeleteOperation request from [%s]", context.peer())

        try:
            name = request.name

            instance_name = self._parse_instance_name(name)
            instance = self._get_instance(instance_name)

            operation_name = self._parse_operation_name(name)
            instance.delete_operation(operation_name)

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return Empty()

    def CancelOperation(self, request, context):
        self.__logger.debug("CancelOperation request from [%s]", context.peer())

        try:
            name = request.name

            instance_name = self._parse_instance_name(name)
            instance = self._get_instance(instance_name)

            operation_name = self._parse_operation_name(name)
            instance.cancel_operation(operation_name)

        except NotImplementedError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return Empty()

    def _parse_instance_name(self, name):
        """ If the instance name is not blank, 'name' will have the form
        {instance_name}/{operation_uuid}. Otherwise, it will just be
        {operation_uuid} """
        names = name.split('/')
        return '/'.join(names[:-1]) if len(names) > 1 else ''

    def _parse_operation_name(self, name):
        names = name.split('/')
        return names[-1] if len(names) > 1 else name

    def _get_instance(self, name):
        try:
            return self._instances[name]

        except KeyError:
            raise InvalidArgumentError("Instance doesn't exist on server: [{}]".format(name))
