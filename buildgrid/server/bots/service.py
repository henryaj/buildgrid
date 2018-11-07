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

from google.protobuf import empty_pb2, timestamp_pb2

from buildgrid._enums import BotStatus
from buildgrid._exceptions import InvalidArgumentError, OutOfSyncError
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2_grpc


class BotsService(bots_pb2_grpc.BotsServicer):

    def __init__(self, server, monitor=False):
        self.__logger = logging.getLogger(__name__)

        self.__bots_by_status = None
        self.__bots_by_instance = None
        self.__bots = None

        self._instances = {}

        bots_pb2_grpc.add_BotsServicer_to_server(self, server)

        self._is_instrumented = monitor

        if self._is_instrumented:
            self.__bots_by_status = {}
            self.__bots_by_instance = {}
            self.__bots = {}

            self.__bots_by_status[BotStatus.OK] = set()
            self.__bots_by_status[BotStatus.UNHEALTHY] = set()

    # --- Public API ---

    def add_instance(self, instance_name, instance):
        self._instances[instance_name] = instance

        if self._is_instrumented:
            self.__bots_by_instance[instance_name] = set()

    # --- Public API: Servicer ---

    def CreateBotSession(self, request, context):
        """Handles CreateBotSessionRequest messages.

        Args:
            request (CreateBotSessionRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.debug("CreateBotSession request from [%s]", context.peer())

        instance_name = request.parent
        bot_status = BotStatus(request.bot_session.status)
        bot_id = request.bot_session.bot_id

        try:
            instance = self._get_instance(instance_name)
            bot_session = instance.create_bot_session(instance_name,
                                                      request.bot_session)
            now = timestamp_pb2.Timestamp()
            now.GetCurrentTime()

            if self._is_instrumented:
                self.__bots[bot_id] = now
                self.__bots_by_instance[instance_name].add(bot_id)
                if bot_status in self.__bots_by_status:
                    self.__bots_by_status[bot_status].add(bot_id)

            return bot_session

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return bots_pb2.BotSession()

    def UpdateBotSession(self, request, context):
        """Handles UpdateBotSessionRequest messages.

        Args:
            request (UpdateBotSessionRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.debug("UpdateBotSession request from [%s]", context.peer())

        names = request.name.split("/")
        bot_status = BotStatus(request.bot_session.status)
        bot_id = request.bot_session.bot_id

        try:
            instance_name = '/'.join(names[:-1])

            instance = self._get_instance(instance_name)
            bot_session = instance.update_bot_session(request.name,
                                                      request.bot_session)

            if self._is_instrumented:
                self.__bots[bot_id].GetCurrentTime()
                if bot_id not in self.__bots_by_status[bot_status]:
                    if bot_status == BotStatus.OK:
                        self.__bots_by_status[BotStatus.OK].add(bot_id)
                        self.__bots_by_status[BotStatus.UNHEALTHY].discard(bot_id)

                    elif bot_status == BotStatus.UNHEALTHY:
                        self.__bots_by_status[BotStatus.OK].discard(bot_id)
                        self.__bots_by_status[BotStatus.UNHEALTHY].add(bot_id)

                    else:
                        self.__bots_by_instance[instance_name].remove(bot_id)
                        del self.__bots[bot_id]

            return bot_session

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except OutOfSyncError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.DATA_LOSS)

        except NotImplementedError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        return bots_pb2.BotSession()

    def PostBotEventTemp(self, request, context):
        """Handles PostBotEventTempRequest messages.

        Args:
            request (PostBotEventTempRequest): The incoming RPC request.
            context (grpc.ServicerContext): Context for the RPC call.
        """
        self.__logger.debug("PostBotEventTemp request from [%s]", context.peer())

        context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        return empty_pb2.Empty()

    # --- Public API: Monitoring ---

    @property
    def is_instrumented(self):
        return self._is_instrumented

    def query_n_bots(self):
        if self.__bots is not None:
            return len(self.__bots)

        return 0

    def query_n_bots_for_instance(self, instance_name):
        try:
            if self.__bots_by_instance is not None:
                return len(self.__bots_by_instance[instance_name])
        except KeyError:
            pass
        return 0

    def query_n_bots_for_status(self, bot_status):
        try:
            if self.__bots_by_status is not None:
                return len(self.__bots_by_status[bot_status])
        except KeyError:
            pass
        return 0

    # --- Private API ---

    def _get_instance(self, name):
        try:
            return self._instances[name]

        except KeyError:
            raise InvalidArgumentError("Instance doesn't exist on server: [{}]".format(name))
