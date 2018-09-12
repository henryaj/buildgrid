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
ActionCacheService
==================

Allows clients to manually query/update the action cache.
"""

import logging

import grpc

from buildgrid._exceptions import InvalidArgumentError, NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc


class ActionCacheService(remote_execution_pb2_grpc.ActionCacheServicer):

    def __init__(self, server):
        self.logger = logging.getLogger(__name__)

        self._instances = {}

        remote_execution_pb2_grpc.add_ActionCacheServicer_to_server(self, server)

    def add_instance(self, name, instance):
        self._instances[name] = instance

    def GetActionResult(self, request, context):
        try:
            instance = self._get_instance(request.instance_name)
            return instance.get_action_result(request.action_digest)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotFoundError as e:
            self.logger.error(e)
            context.set_code(grpc.StatusCode.NOT_FOUND)

        return remote_execution_pb2.ActionResult()

    def UpdateActionResult(self, request, context):
        try:
            instance = self._get_instance(request.instance_name)
            instance.update_action_result(request.action_digest, request.action_result)
            return request.action_result

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotImplementedError as e:
            self.logger.error(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        return remote_execution_pb2.ActionResult()

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError("Invalid instance name: [{}]".format(instance_name))
