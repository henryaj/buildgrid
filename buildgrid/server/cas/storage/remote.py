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

from buildgrid.client.cas import download, upload
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.rpc import code_pb2
from buildgrid._protos.google.rpc import status_pb2
from buildgrid.settings import HASH

from .storage_abc import StorageABC


class RemoteStorage(StorageABC):

    def __init__(self, channel, instance_name):
        self.__logger = logging.getLogger(__name__)

        self.instance_name = instance_name
        self.channel = channel

        self._stub_cas = remote_execution_pb2_grpc.ContentAddressableStorageStub(channel)

    def has_blob(self, digest):
        self.__logger.debug("Checking for blob: [{}]".format(digest))
        if not self.missing_blobs([digest]):
            return True
        return False

    def get_blob(self, digest):
        self.__logger.debug("Getting blob: [{}]".format(digest))
        with download(self.channel, instance=self.instance_name) as downloader:
            blob = downloader.get_blob(digest)
            if blob is not None:
                return io.BytesIO(blob)
            else:
                return None

    def delete_blob(self, digest):
        """ The REAPI doesn't have a deletion method, so we can't support
        deletion for remote storage.
        """
        raise NotImplementedError(
            "Deletion is not supported for remote storage!")

    def begin_write(self, digest):
        return io.BytesIO()

    def commit_write(self, digest, write_session):
        self.__logger.debug("Writing blob: [{}]".format(digest))
        with upload(self.channel, instance=self.instance_name) as uploader:
            uploader.put_blob(write_session.getvalue())

    def missing_blobs(self, blobs):
        if len(blobs) > 100:
            self.__logger.debug(
                "Missing blobs request for: {} (truncated)".format(blobs[:100]))
        else:
            self.__logger.debug("Missing blobs request for: {}".format(blobs))
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
