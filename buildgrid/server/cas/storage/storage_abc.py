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
#
# Authors:
#        Carter Sande <csande@bloomberg.net>

"""
StorageABC
==================

The abstract base class for storage providers.
"""

import abc

from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from buildgrid._protos.google.rpc.status_pb2 import Status
from buildgrid._protos.google.rpc import code_pb2

from ....settings import HASH


class StorageABC(abc.ABC):

    @abc.abstractmethod
    def has_blob(self, digest):
        """Return True if the blob with the given instance/digest exists."""
        pass

    @abc.abstractmethod
    def get_blob(self, digest):
        """Return a file-like object containing the blob.

        If the blob isn't present in storage, return None.
        """
        pass

    @abc.abstractmethod
    def begin_write(self, digest):
        """Return a file-like object to which a blob's contents could be
        written.
        """
        pass

    @abc.abstractmethod
    def commit_write(self, digest, write_session):
        """Commit the write operation. `write_session` must be an object
        returned by `begin_write`.

        The storage object is not responsible for verifying that the data
        written to the write_session actually matches the digest. The caller
        must do that.
        """
        pass

    def missing_blobs(self, digests):
        """Return a container containing the blobs not present in CAS."""
        result = []
        for digest in digests:
            if not self.has_blob(digest):
                result.append(digest)
        return result

    def bulk_update_blobs(self, blobs):
        """Given a container of (digest, value) tuples, add all the blobs
        to CAS. Return a list of Status objects corresponding to the
        result of uploading each of the blobs.

        Unlike in `commit_write`, the storage object will verify that each of
        the digests matches the provided data.
        """
        result = []
        for digest, data in blobs:
            if len(data) != digest.size_bytes or HASH(data).hexdigest() != digest.hash:
                result.append(
                    Status(
                        code=code_pb2.INVALID_ARGUMENT,
                        message="Data doesn't match hash",
                    ))
            else:
                try:
                    write_session = self.begin_write(digest)
                    write_session.write(data)
                    self.commit_write(digest, write_session)
                except Exception as ex:
                    result.append(Status(code=code_pb2.UNKNOWN, message=str(ex)))
                else:
                    result.append(Status(code=code_pb2.OK))
        return result

    def put_message(self, message):
        """Store the given Protobuf message in CAS, returning its digest."""
        message_blob = message.SerializeToString()
        digest = Digest(hash=HASH(message_blob).hexdigest(), size_bytes=len(message_blob))
        session = self.begin_write(digest)
        session.write(message_blob)
        self.commit_write(digest, session)
        return digest

    def get_message(self, digest, message_type):
        """Retrieve the Protobuf message with the given digest and type from
        CAS. If the blob is not present, returns None.
        """
        message_blob = self.get_blob(digest)
        if message_blob is None:
            return None
        result = message_type.FromString(message_blob.read())
        message_blob.close()
        return result
