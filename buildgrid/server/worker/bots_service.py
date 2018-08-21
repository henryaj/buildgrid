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
BotsService
=================

"""

import logging

import grpc

from google.protobuf.empty_pb2 import Empty

from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2_grpc

from .._exceptions import InvalidArgumentError, OutofSyncError


class BotsService(bots_pb2_grpc.BotsServicer):

    def __init__(self, instances):
        self._instances = instances
        self.logger = logging.getLogger(__name__)

    def CreateBotSession(self, request, context):
        try:
            parent = request.parent
            instance = self._get_instance(request.parent)
            return instance.create_bot_session(parent,
                                               request.bot_session)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return bots_pb2.BotSession()

    def UpdateBotSession(self, request, context):
        try:
            names = request.name.split("/")
            # Operation name should be in format:
            # {instance/name}/{uuid}
            instance_name = ''.join(names[0:-1])

            instance = self._get_instance(instance_name)
            return instance.update_bot_session(request.name,
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

        return bots_pb2.BotSession()

    def PostBotEventTemp(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        return Empty()

    def _get_instance(self, name):
        try:
            return self._instances[name]

        except KeyError:
            raise InvalidArgumentError("Instance doesn't exist on server: {}".format(name))
