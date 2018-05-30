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
OperationsService
===============

"""


import grpc
from google.longrunning import operations_pb2_grpc

class OperationsService(operations_pb2_grpc.OperationsServicer):

    def __init__(self, instance):
        if instance is None:
            raise TypeError
        else:
            self._instance = instance

    def GetOperation(self, request, context):
        try:
            return self._instance.get_operation(request.name)
        except Exception as e:
            print("Exception: {}".format(e))

    def ListOperations(self, request, context):
        try:
            return self._instance.list_operations(request.name,
                                                  request.filter,
                                                  request.page_size,
                                                  request.page_token)
        except Exception as e:
            print("Exception: {}".format(e))

    def DeleteOperation(self, request, context):
        try:
            return self._instance.delete_operation(request.name)
        except Exception as e:
            print("Exception: {}".format(e))

    def CancelOperation(self, request, context):
        try:
            return self._instance.delete_operation(request.name)
        except Exception as e:
            print("Exception: {}".format(e))
