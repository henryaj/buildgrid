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
Bot Interface
====

Interface to grpc
"""


import logging

from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2, bots_pb2_grpc

from .._exceptions import BotError

class BotInterface:
    """ Interface handles calls to the server.
    """

    def __init__(self, channel):
        self.logger = logging.getLogger(__name__)
        self.logger.info(channel)
        self._stub = bots_pb2_grpc.BotsStub(channel)

    def create_bot_session(self, parent, bot_session):
        request = bots_pb2.CreateBotSessionRequest(parent = parent,
                                                   bot_session = bot_session)
        return self._stub.CreateBotSession(request)

    def update_bot_session(self, bot_session, update_mask = None):
        request = bots_pb2.UpdateBotSessionRequest(name = bot_session.name,
                                                   bot_session = bot_session,
                                                   update_mask = update_mask)
        return self._stub.UpdateBotSession(request)