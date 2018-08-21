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
ByteStreamService
==================

Implements the ByteStream API, which clients can use to read and write
CAS blobs.
"""

import grpc

from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2 as re_pb2

from ...settings import HASH


class ByteStreamService(bytestream_pb2_grpc.ByteStreamServicer):

    BLOCK_SIZE = 1 * 1024 * 1024  # 1 MB block size

    def __init__(self, storage):
        self._storage = storage

    def Read(self, request, context):
        # Only one instance for now.
        storage = self._storage

        # Parse/verify resource name.
        # Read resource names look like "[instance/]blobs/abc123hash/99".
        path = request.resource_name.split("/")
        if len(path) == 3:
            path = [""] + path
        if len(path) != 4 or path[1] != "blobs" or not path[3].isdigit():
            context.abort(grpc.StatusCode.NOT_FOUND, "Invalid resource name")
        # instance_name = path[0]
        digest = re_pb2.Digest(hash=path[2], size_bytes=int(path[3]))

        # Check the given read offset and limit.
        if request.read_offset < 0 or request.read_offset > digest.size_bytes:
            context.abort(grpc.StatusCode.OUT_OF_RANGE, "Read offset out of range")
        elif request.read_limit == 0:
            bytes_remaining = digest.size_bytes - request.read_offset
        elif request.read_limit > 0:
            bytes_remaining = request.read_limit
        else:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Negative read_limit is invalid")

        # Read the blob from storage and send its contents to the client.
        result = storage.get_blob(digest)
        if result is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "Blob not found")
        elif result.seekable():
            result.seek(request.read_offset)
        else:
            result.read(request.read_offset)
        while bytes_remaining > 0:
            yield bytestream_pb2.ReadResponse(
                data=result.read(min(self.BLOCK_SIZE, bytes_remaining)))
            bytes_remaining -= self.BLOCK_SIZE

    def Write(self, request_iterator, context):
        # Only one instance for now.
        storage = self._storage

        requests = iter(request_iterator)
        first_request = next(requests)
        if first_request.write_offset != 0:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Nonzero write offset is unsupported")

        # Parse/verify resource name.
        # Write resource names look like "[instance/]uploads/SOME-GUID/blobs/abc123hash/99".
        path = first_request.resource_name.split("/")
        if path[0] == "uploads":
            path = [""] + path
        if len(path) < 6 or path[1] != "uploads" or path[3] != "blobs" or not path[5].isdigit():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid resource name")
        # instance_name = path[0]
        digest = re_pb2.Digest(hash=path[4], size_bytes=int(path[5]))

        # Start the write session and write the first request's data.
        write_session = storage.begin_write(digest)
        write_session.write(first_request.data)
        hash_ = HASH(first_request.data)
        bytes_written = len(first_request.data)
        done = first_request.finish_write

        # Handle subsequent write requests.
        for request in requests:
            if done:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              "Write request sent after write finished")
            elif request.write_offset != bytes_written:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid write offset")
            elif request.resource_name and request.resource_name != first_request.resource_name:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Resource name changed mid-write")
            done = request.finish_write
            bytes_written += len(request.data)
            if bytes_written > digest.size_bytes:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Wrote too much data to blob")
            write_session.write(request.data)
            hash_.update(request.data)

        # Check that the data matches the provided digest.
        if bytes_written != digest.size_bytes or not done:
            context.abort(grpc.StatusCode.UNIMPLEMENTED,
                          "Cannot close stream before finishing write")
        elif hash_.hexdigest() != digest.hash:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Data does not match hash")
        storage.commit_write(digest, write_session)
        return bytestream_pb2.WriteResponse(committed_size=bytes_written)
