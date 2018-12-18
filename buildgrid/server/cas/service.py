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
CAS services
==================

Implements the Content Addressable Storage API and ByteStream API.
"""


import logging

import grpc

from buildgrid._exceptions import InvalidArgumentError, NotFoundError, OutOfRangeError
from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc
from buildgrid.server._authentication import AuthContext, authorize


class ContentAddressableStorageService(remote_execution_pb2_grpc.ContentAddressableStorageServicer):

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        remote_execution_pb2_grpc.add_ContentAddressableStorageServicer_to_server(self, server)

    # --- Public API ---

    def add_instance(self, name, instance):
        self._instances[name] = instance

    # --- Public API: Servicer ---

    @authorize(AuthContext)
    def FindMissingBlobs(self, request, context):
        self.__logger.debug("FindMissingBlobs request from [%s]", context.peer())

        try:
            instance = self._get_instance(request.instance_name)
            response = instance.find_missing_blobs(request.blob_digests)

            return response

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return remote_execution_pb2.FindMissingBlobsResponse()

    @authorize(AuthContext)
    def BatchUpdateBlobs(self, request, context):
        self.__logger.debug("BatchUpdateBlobs request from [%s]", context.peer())

        try:
            instance = self._get_instance(request.instance_name)
            response = instance.batch_update_blobs(request.requests)

            return response

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return remote_execution_pb2.BatchReadBlobsResponse()

    @authorize(AuthContext)
    def BatchReadBlobs(self, request, context):
        self.__logger.debug("BatchReadBlobs request from [%s]", context.peer())

        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')

        return remote_execution_pb2.BatchReadBlobsResponse()

    @authorize(AuthContext)
    def GetTree(self, request, context):
        self.__logger.debug("GetTree request from [%s]", context.peer())

        try:
            instance = self._get_instance(request.instance_name)
            yield from instance.get_tree(request)

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

            yield remote_execution_pb2.GetTreeResponse()

    # --- Private API ---

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError("Invalid instance name: [{}]".format(instance_name))


class ByteStreamService(bytestream_pb2_grpc.ByteStreamServicer):

    def __init__(self, server):
        self.__logger = logging.getLogger(__name__)

        self._instances = {}

        bytestream_pb2_grpc.add_ByteStreamServicer_to_server(self, server)

    # --- Public API ---

    def add_instance(self, name, instance):
        self._instances[name] = instance

    # --- Public API: Servicer ---

    @authorize(AuthContext)
    def Read(self, request, context):
        self.__logger.debug("Read request from [%s]", context.peer())

        names = request.resource_name.split('/')

        try:
            instance_name = ''
            # Format: "{instance_name}/blobs/{hash}/{size}":
            if len(names) < 3 or names[-3] != 'blobs':
                raise InvalidArgumentError("Invalid resource name: [{}]"
                                           .format(request.resource_name))

            elif names[0] != 'blobs':
                index = names.index('blobs')
                instance_name = '/'.join(names[:index])
                names = names[index:]

            if len(names) < 3:
                raise InvalidArgumentError("Invalid resource name: [{}]"
                                           .format(request.resource_name))

            hash_, size_bytes = names[1], names[2]

            instance = self._get_instance(instance_name)

            yield from instance.read(hash_, size_bytes,
                                     request.read_offset, request.read_limit)

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield bytestream_pb2.ReadResponse()

        except NotFoundError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.NOT_FOUND)
            yield bytestream_pb2.ReadResponse()

        except OutOfRangeError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            yield bytestream_pb2.ReadResponse()

    @authorize(AuthContext)
    def Write(self, requests, context):
        self.__logger.debug("Write request from [%s]", context.peer())

        request = next(requests)
        names = request.resource_name.split('/')

        try:
            instance_name = ''
            # Format: "{instance_name}/uploads/{uuid}/blobs/{hash}/{size}/{anything}":
            if len(names) < 5 or 'uploads' not in names or 'blobs' not in names:
                raise InvalidArgumentError("Invalid resource name: [{}]"
                                           .format(request.resource_name))

            elif names[0] != 'uploads':
                index = names.index('uploads')
                instance_name = '/'.join(names[:index])
                names = names[index:]

            if len(names) < 5:
                raise InvalidArgumentError("Invalid resource name: [{}]"
                                           .format(request.resource_name))

            _, hash_, size_bytes = names[1], names[3], names[4]

            instance = self._get_instance(instance_name)

            return instance.write(hash_, size_bytes, request.data,
                                  [request.data for request in requests])

        except NotImplementedError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        except InvalidArgumentError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotFoundError as e:
            self.__logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.NOT_FOUND)

        return bytestream_pb2.WriteResponse()

    @authorize(AuthContext)
    def QueryWriteStatus(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')

        return bytestream_pb2.QueryWriteStatusResponse()

    # --- Private API ---

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError("Invalid instance name: [{}]".format(instance_name))
