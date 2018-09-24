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


from itertools import tee
import logging

import grpc

from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc

from .._exceptions import InvalidArgumentError, NotFoundError, OutOfRangeError


class ContentAddressableStorageService(remote_execution_pb2_grpc.ContentAddressableStorageServicer):

    def __init__(self, server):
        self.logger = logging.getLogger(__name__)

        self._instances = {}

        remote_execution_pb2_grpc.add_ContentAddressableStorageServicer_to_server(self, server)

    def add_instance(self, name, instance):
        self._instances[name] = instance

    def FindMissingBlobs(self, request, context):
        try:
            instance = self._get_instance(request.instance_name)
            return instance.find_missing_blobs(request.blob_digests)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return remote_execution_pb2.FindMissingBlobsResponse()

    def BatchUpdateBlobs(self, request, context):
        try:
            instance = self._get_instance(request.instance_name)
            return instance.batch_update_blobs(request.requests)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        return remote_execution_pb2.BatchReadBlobsResponse()

    def BatchReadBlobs(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')

        return remote_execution_pb2.BatchReadBlobsResponse()

    def GetTree(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')

        return iter([remote_execution_pb2.GetTreeResponse()])

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError("Invalid instance name: [{}]".format(instance_name))


class ByteStreamService(bytestream_pb2_grpc.ByteStreamServicer):

    def __init__(self, server):
        self.logger = logging.getLogger(__name__)

        self._instances = {}

        bytestream_pb2_grpc.add_ByteStreamServicer_to_server(self, server)

    def add_instance(self, name, instance):
        self._instances[name] = instance

    def Read(self, request, context):
        try:
            path = request.resource_name.split("/")
            instance_name = path[0]

            # TODO: Decide on default instance name
            if path[0] == "blobs":
                if len(path) < 3 or not path[2].isdigit():
                    raise InvalidArgumentError("Invalid resource name: [{}]".format(request.resource_name))
                instance_name = ""

            elif path[1] == "blobs":
                if len(path) < 4 or not path[3].isdigit():
                    raise InvalidArgumentError("Invalid resource name: [{}]".format(request.resource_name))

            else:
                raise InvalidArgumentError("Invalid resource name: [{}]".format(request.resource_name))

            instance = self._get_instance(instance_name)
            yield from instance.read(path,
                                     request.read_offset,
                                     request.read_limit)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            yield bytestream_pb2.ReadResponse()

        except NotFoundError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.NOT_FOUND)
            yield bytestream_pb2.ReadResponse()

        except OutOfRangeError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.OUT_OF_RANGE)
            yield bytestream_pb2.ReadResponse()

    def Write(self, requests, context):
        try:
            requests, request_probe = tee(requests, 2)
            first_request = next(request_probe)

            path = first_request.resource_name.split("/")

            instance_name = path[0]

            # TODO: Sort out no instance name
            if path[0] == "uploads":
                if len(path) < 5 or path[2] != "blobs" or not path[4].isdigit():
                    raise InvalidArgumentError("Invalid resource name: [{}]".format(first_request.resource_name))
                instance_name = ""

            elif path[1] == "uploads":
                if len(path) < 6 or path[3] != "blobs" or not path[5].isdigit():
                    raise InvalidArgumentError("Invalid resource name: [{}]".format(first_request.resource_name))

            else:
                raise InvalidArgumentError("Invalid resource name: [{}]".format(first_request.resource_name))

            instance = self._get_instance(instance_name)
            return instance.write(requests)

        except NotImplementedError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)

        except InvalidArgumentError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)

        except NotFoundError as e:
            self.logger.error(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.NOT_FOUND)

        return bytestream_pb2.WriteResponse()

    def QueryWriteStatus(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')

        return bytestream_pb2.QueryWriteStatusResponse()

    def _get_instance(self, instance_name):
        try:
            return self._instances[instance_name]

        except KeyError:
            raise InvalidArgumentError("Invalid instance name: [{}]".format(instance_name))
