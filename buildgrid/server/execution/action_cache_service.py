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

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc

from .._exceptions import NotFoundError


class ActionCacheService(remote_execution_pb2_grpc.ActionCacheServicer):

    def __init__(self, action_cache):
        self._action_cache = action_cache
        self.logger = logging.getLogger(__name__)

    def GetActionResult(self, request, context):
        try:
            return self._action_cache.get_action_result(request.action_digest)

        except NotFoundError as e:
            self.logger.error(e)
            context.set_code(grpc.StatusCode.NOT_FOUND)

        return remote_execution_pb2.ActionResult()

    def UpdateActionResult(self, request, context):
        try:
            self._action_cache.update_action_result(request.action_digest, request.action_result)
            return request.action_result

        except NotImplementedError as e:
            self.logger.error(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        return remote_execution_pb2.ActionResult()
