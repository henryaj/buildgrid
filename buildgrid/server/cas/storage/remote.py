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

from buildgrid.client.cas import upload
from buildgrid._protos.google.bytestream import bytestream_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.rpc import code_pb2
from buildgrid._protos.google.rpc import status_pb2
from buildgrid.utils import gen_fetch_blob
from buildgrid.settings import HASH

from .storage_abc import StorageABC


class RemoteStorage(StorageABC):

    def __init__(self, channel, instance_name):
        self.logger = logging.getLogger(__name__)

        self.instance_name = instance_name
        self.channel = channel

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

            for data in gen_fetch_blob(self._stub_bs, digest, self.instance_name):
                length += fetched_data.write(data)

            assert digest.size_bytes == length
            fetched_data.seek(0)
            return fetched_data

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                pass
            else:
                self.logger.error(e.details())
                raise

        return None

    def begin_write(self, digest):
        return io.BytesIO()

    def commit_write(self, digest, write_session):
        with upload(self.channel, instance=self.instance_name) as uploader:
            uploader.put_blob(write_session.getvalue())

    def missing_blobs(self, blobs):
        request = remote_execution_pb2.FindMissingBlobsRequest(instance_name=self.instance_name)

        for blob in blobs:
            request_digest = request.blob_digests.add()
            request_digest.hash = blob.hash
            request_digest.size_bytes = blob.size_bytes

        response = self._stub_cas.FindMissingBlobs(request)

        return [x for x in response.missing_blob_digests]

    def bulk_update_blobs(self, blobs):
        sent_digests = []
        with upload(self.channel, instance=self.instance_name) as uploader:
            for digest, blob in blobs:
                if len(blob) != digest.size_bytes or HASH(blob).hexdigest() != digest.hash:
                    sent_digests.append(remote_execution_pb2.Digest())
                else:
                    sent_digests.append(uploader.put_blob(blob, digest=digest, queue=True))

        assert len(sent_digests) == len(blobs)

        return [status_pb2.Status(code=code_pb2.OK) if d.ByteSize() > 0
                else status_pb2.Status(code=code_pb2.UNKNOWN) for d in sent_digests]
