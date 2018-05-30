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
import sys

from .interface.bots_interface import BotsInterface

from google.devtools.remoteworkers.v1test2 import bots_pb2, bots_pb2_grpc

class BotsService(bots_pb2_grpc.BotsServicer):
    
    def __init__(self, instance):
        self._instance = instance

    def CreateBotSession(self, request, context):
        try:
            return self._instance.create_bot_session(request.parent,
                                                      request.bot_session)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Exception: {}\n{} {} {}".format(e, exc_type, exc_obj, exc_tb))
        return

    def UpdateBotSession(self, request, context):
        try:
            return self._instance.update_bot_session(request.name,
                                                     request.bot_session)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            print("Exception: {}\n{} {} {}".format(e, exc_type, exc_obj, exc_tb))
        return

    def PostBotEventTemp(self, request, context):
        raise NotImplementedError
