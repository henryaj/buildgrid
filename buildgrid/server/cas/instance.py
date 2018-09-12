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
Storage Instances
=========
Instances of CAS and ByteStream
"""

from buildgrid._exceptions import InvalidArgumentError, NotFoundError, OutOfRangeError
from buildgrid._protos.google.bytestream import bytestream_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2 as re_pb2
from buildgrid.settings import HASH


class ContentAddressableStorageInstance:

    def __init__(self, storage):
        self._storage = storage

    def register_instance_with_server(self, instance_name, server):
        server.add_cas_instance(self, instance_name)

    def find_missing_blobs(self, blob_digests):
        storage = self._storage
        return re_pb2.FindMissingBlobsResponse(
            missing_blob_digests=storage.missing_blobs(blob_digests))

    def batch_update_blobs(self, requests):
        storage = self._storage
        store = []
        for request_proto in requests:
            store.append((request_proto.digest, request_proto.data))

        response = re_pb2.BatchUpdateBlobsResponse()
        statuses = storage.bulk_update_blobs(store)

        for (digest, _), status in zip(store, statuses):
            response_proto = response.responses.add()
            response_proto.digest.CopyFrom(digest)
            response_proto.status.CopyFrom(status)

        return response


class ByteStreamInstance:

    BLOCK_SIZE = 1 * 1024 * 1024  # 1 MB block size

    def __init__(self, storage):
        self._storage = storage

    def register_instance_with_server(self, instance_name, server):
        server.add_bytestream_instance(self, instance_name)

    def read(self, path, read_offset, read_limit):
        storage = self._storage

        if path[0] == "blobs":
            path = [""] + path

        # Parse/verify resource name.
        # Read resource names look like "[instance/]blobs/abc123hash/99".
        digest = re_pb2.Digest(hash=path[2], size_bytes=int(path[3]))

        # Check the given read offset and limit.
        if read_offset < 0 or read_offset > digest.size_bytes:
            raise OutOfRangeError("Read offset out of range")

        elif read_limit == 0:
            bytes_remaining = digest.size_bytes - read_offset

        elif read_limit > 0:
            bytes_remaining = read_limit

        else:
            raise InvalidArgumentError("Negative read_limit is invalid")

        # Read the blob from storage and send its contents to the client.
        result = storage.get_blob(digest)
        if result is None:
            raise NotFoundError("Blob not found")

        elif result.seekable():
            result.seek(read_offset)

        else:
            result.read(read_offset)

        while bytes_remaining > 0:
            yield bytestream_pb2.ReadResponse(
                data=result.read(min(self.BLOCK_SIZE, bytes_remaining)))
            bytes_remaining -= self.BLOCK_SIZE

    def write(self, requests):
        storage = self._storage

        first_request = next(requests)
        path = first_request.resource_name.split("/")

        if path[0] == "uploads":
            path = [""] + path

        digest = re_pb2.Digest(hash=path[4], size_bytes=int(path[5]))
        write_session = storage.begin_write(digest)

        # Start the write session and write the first request's data.
        write_session.write(first_request.data)
        hash_ = HASH(first_request.data)
        bytes_written = len(first_request.data)
        finished = first_request.finish_write

        # Handle subsequent write requests.
        while not finished:

            for request in requests:
                if finished:
                    raise InvalidArgumentError("Write request sent after write finished")

                elif request.write_offset != bytes_written:
                    raise InvalidArgumentError("Invalid write offset")

                elif request.resource_name and request.resource_name != first_request.resource_name:
                    raise InvalidArgumentError("Resource name changed mid-write")

                finished = request.finish_write
                bytes_written += len(request.data)
                if bytes_written > digest.size_bytes:
                    raise InvalidArgumentError("Wrote too much data to blob")

                write_session.write(request.data)
                hash_.update(request.data)

        # Check that the data matches the provided digest.
        if bytes_written != digest.size_bytes or not finished:
            raise NotImplementedError("Cannot close stream before finishing write")

        elif hash_.hexdigest() != digest.hash:
            raise InvalidArgumentError("Data does not match hash")

        storage.commit_write(digest, write_session)
        return bytestream_pb2.WriteResponse(committed_size=bytes_written)
