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


import logging

import grpc

from buildgrid._exceptions import InvalidArgumentError, NotFoundError
from buildgrid._protos.buildstream.v2 import buildstream_pb2
from buildgrid._protos.buildstream.v2 import buildstream_pb2_grpc


class ReferenceStorageService(buildstream_pb2_grpc.ReferenceStorageServicer):

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        buildstream_pb2_grpc.add_ReferenceStorageServicer_to_server(self, server)

    def add_instance(self, name, instance):
        self._instances[name] = instance

    def GetReference(self, request, context):
        try:
            instance = self._get_instance(request.instance_name)
            digest = instance.get_digest_reference(request.key)
            response = buildstream_pb2.GetReferenceResponse()
            response.digest.CopyFrom(digest)
            return response

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotFoundError as e:
            self.__logger.debug(e)
            context.set_code(grpc.StatusCode.NOT_FOUND)

        return buildstream_pb2.GetReferenceResponse()

    def UpdateReference(self, request, context):
        try:
            instance = self._get_instance(request.instance_name)
            digest = request.digest

            for key in request.keys:
                instance.update_reference(key, digest)

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotImplementedError:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        return buildstream_pb2.UpdateReferenceResponse()

    def Status(self, request, context):
        try:
            instance = self._get_instance(request.instance_name)
            allow_updates = instance.allow_updates
            return buildstream_pb2.StatusResponse(allow_updates=allow_updates)

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return buildstream_pb2.StatusResponse()

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError("Invalid instance name: [{}]".format(instance_name))
