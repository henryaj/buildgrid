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
RemoteStorage
==================

Forwwards storage requests to a remote storage.
"""

import io
import logging

import grpc

from buildgrid.utils import gen_fetch_blob, gen_write_request_blob
from buildgrid._protos.google.bytestream import bytestream_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc

from .storage_abc import StorageABC


class RemoteStorage(StorageABC):

    def __init__(self, channel, instance_name):
        self.logger = logging.getLogger(__name__)
        self._instance_name = instance_name
        self._stub_bs = bytestream_pb2_grpc.ByteStreamStub(channel)
        self._stub_cas = remote_execution_pb2_grpc.ContentAddressableStorageStub(channel)

    def has_blob(self, digest):
        if not self.missing_blobs([digest]):
            return True
        return False

    def get_blob(self, digest):
        try:
            fetched_data = io.BytesIO()
            length = 0

            for data in gen_fetch_blob(self._stub_bs, digest, self._instance_name):
                length += fetched_data.write(data)

            if length:
                assert digest.size_bytes == length
                fetched_data.seek(0)
                return fetched_data

            else:
                return None

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                pass
            else:
                self.logger.error(e.details())
                raise

        return None

    def begin_write(self, digest):
        return io.BytesIO(digest.SerializeToString())

    def commit_write(self, digest, write_session):
        write_session.seek(0)

        for request in gen_write_request_blob(write_session, digest, self._instance_name):
            self._stub_bs.Write(request)

    def missing_blobs(self, blobs):
        request = remote_execution_pb2.FindMissingBlobsRequest(instance_name=self._instance_name)

        for blob in blobs:
            request_digest = request.blob_digests.add()
            request_digest.hash = blob.hash
            request_digest.size_bytes = blob.size_bytes

        response = self._stub_cas.FindMissingBlobs(request)

        return [x for x in response.missing_blob_digests]

    def bulk_update_blobs(self, blobs):
        request = remote_execution_pb2.BatchUpdateBlobsRequest(instance_name=self._instance_name)

        for digest, data in blobs:
            reqs = request.requests.add()
            reqs.digest.CopyFrom(digest)
            reqs.data = data

        response = self._stub_cas.BatchUpdateBlobs(request)

        responses = response.responses

        # Check everything was sent back, even if order changed
        assert ([x.digest for x in request.requests].sort(key=lambda x: x.hash)) == \
            ([x.digest for x in responses].sort(key=lambda x: x.hash))

        return [x.status for x in responses]
