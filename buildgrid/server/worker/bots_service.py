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
BotsService
=================

"""

import grpc
import logging

from .bots_interface import BotsInterface
from ._exceptions import InvalidArgumentError, OutofSyncError

from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2, bots_pb2_grpc

class BotsService(bots_pb2_grpc.BotsServicer):

    def __init__(self, instance):
        self._instance = instance
        self.logger = logging.getLogger(__name__)

    def CreateBotSession(self, request, context):
        try:
            return self._instance.create_bot_session(request.parent,
                                                     request.bot_session)
        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

    def UpdateBotSession(self, request, context):
        try:
            return self._instance.update_bot_session(request.name,
                                                     request.bot_session)
        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except OutofSyncError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.DATA_LOSS)

        except NotImplementedError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

    def PostBotEventTemp(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
