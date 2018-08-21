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

from buildgrid._protos.buildstream.v2 import buildstream_pb2
from buildgrid._protos.buildstream.v2 import buildstream_pb2_grpc

from .._exceptions import NotFoundError


class ReferenceStorageService(buildstream_pb2_grpc.ReferenceStorageServicer):

    def __init__(self, reference_cache):
        self._reference_cache = reference_cache
        self.logger = logging.getLogger(__name__)

    def GetReference(self, request, context):
        try:
            response = buildstream_pb2.GetReferenceResponse()
            response.digest.CopyFrom(self._reference_cache.get_digest_reference(request.key))
            return response

        except NotFoundError:
            context.set_code(grpc.StatusCode.NOT_FOUND)

    def UpdateReference(self, request, context):
        try:
            for key in request.keys:
                self._reference_cache.update_reference(key, request.digest)

            return buildstream_pb2.UpdateReferenceResponse()

        except NotImplementedError:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

    def Status(self, request, context):
        allow_updates = self._reference_cache.allow_updates
        return buildstream_pb2.StatusResponse(allow_updates=allow_updates)
