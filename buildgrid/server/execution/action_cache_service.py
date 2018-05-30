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
ActionCacheService
==================

Action Cache currently not implemented.
"""

import grpc

from gdev.devtools.remoteexecution.v1test import remote_execution_pb2, remote_execution_pb2_grpc

class ActionCacheService(remote_execution_pb2_grpc.ActionCacheServicer):

    def GetActionResult(self, request, context):
        raise NotImplementedError

    def UpdateActionResult(self, request, context):
        raise NotImplementedError
