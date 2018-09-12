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


from contextlib import contextmanager
import uuid
import os

from buildgrid.settings import HASH
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc


@contextmanager
def upload(channel, instance=None, u_uid=None):
    uploader = Uploader(channel, instance=instance, u_uid=u_uid)
    try:
        yield uploader
    finally:
        uploader.close()


class Uploader:
    """Remote CAS files, directories and messages upload helper.

    The :class:`Uploader` class comes with a generator factory function that can
    be used together with the `with` statement for context management::

        with upload(channel, instance='build') as cas:
            cas.upload_file('/path/to/local/file')

    Attributes:
        FILE_SIZE_THRESHOLD (int): maximum size for a queueable file.
        MAX_REQUEST_SIZE (int): maximum size for a single gRPC request.
    """

    FILE_SIZE_THRESHOLD = 1 * 1024 * 1024
    MAX_REQUEST_SIZE = 2 * 1024 * 1024

    def __init__(self, channel, instance=None, u_uid=None):
        """Initializes a new :class:`Uploader` instance.

        Args:
            channel (grpc.Channel): A gRPC channel to the CAS endpoint.
            instance (str, optional): the targeted instance's name.
            u_uid (str, optional): a UUID for CAS transactions.
        """
        self.channel = channel

        self.instance_name = instance
        if u_uid is not None:
            self.u_uid = u_uid
        else:
            self.u_uid = str(uuid.uuid4())

        self.__bytestream_stub = bytestream_pb2_grpc.ByteStreamStub(self.channel)
        self.__cas_stub = remote_execution_pb2_grpc.ContentAddressableStorageStub(self.channel)

        self.__requests = dict()
        self.__request_size = 0

    def upload_file(self, file_path, queue=True):
        """Stores a local file into the remote CAS storage.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :method:`flush` can force the request to be
        send immediately (allong with the rest of the queued batch).

        Args:
            file_path (str): absolute or relative path to a local file.
            queue (bool, optional): wheter or not the upload request may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Returns:
            :obj:`Digest`: The digest of the file's content.

        Raises:
            OSError: If `file_path` does not exist or is not readable.
        """
        if not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)

        with open(file_path, 'rb') as bytes_steam:
            file_bytes = bytes_steam.read()

        if not queue or len(file_bytes) > Uploader.FILE_SIZE_THRESHOLD:
            blob_digest = self._send_blob(file_bytes)
        else:
            blob_digest = self._queue_blob(file_bytes)

        return blob_digest

    def upload_directory(self, directory, queue=True):
        """Stores a :obj:`Directory` into the remote CAS storage.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :method:`flush` can force the request to be
        send immediately (allong with the rest of the queued batch).

        Args:
            directory (:obj:`Directory`): a :obj:`Directory` object.
            queue (bool, optional): wheter or not the upload request may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Returns:
            :obj:`Digest`: The digest of the :obj:`Directory`.
        """
        if not isinstance(directory, remote_execution_pb2.Directory):
            raise TypeError

        if not queue:
            return self._send_blob(directory.SerializeToString())
        else:
            return self._queue_blob(directory.SerializeToString())

    def put_message(self, message):
        """Stores a message into the remote CAS storage.

        Message is send immediately, upload is never be deferred.

        Args:
            message (:obj:`Message`): a protobuf message object.

        Returns:
            :obj:`Digest`: The digest of the message.
        """
        return self._send_blob(message.SerializeToString())

    def flush(self):
        """Ensures any queued request gets sent."""
        if self.__requests:
            self._send_batch()

    def close(self):
        """Closes the underlying connection stubs.

        Note:
            This will always send pending requests before closing connections,
            if any.
        """
        self.flush()

        self.__bytestream_stub = None
        self.__cas_stub = None

    def _queue_blob(self, blob):
        """Queues a memory block for later batch upload"""
        blob_digest = remote_execution_pb2.Digest()
        blob_digest.hash = HASH(blob).hexdigest()
        blob_digest.size_bytes = len(blob)

        if self.__request_size + len(blob) > Uploader.MAX_REQUEST_SIZE:
            self._send_batch()

        update_request = remote_execution_pb2.BatchUpdateBlobsRequest.Request()
        update_request.digest.CopyFrom(blob_digest)
        update_request.data = blob

        update_request_size = update_request.ByteSize()
        if self.__request_size + update_request_size > Uploader.MAX_REQUEST_SIZE:
            self._send_batch()

        self.__requests[update_request.digest.hash] = update_request
        self.__request_size += update_request_size

        return blob_digest

    def _send_blob(self, blob):
        """Sends a memory block using ByteStream.Write()"""
        blob_digest = remote_execution_pb2.Digest()
        blob_digest.hash = HASH(blob).hexdigest()
        blob_digest.size_bytes = len(blob)

        if self.instance_name is not None:
            resource_name = '/'.join([self.instance_name, 'uploads', self.u_uid, 'blobs',
                                      blob_digest.hash, str(blob_digest.size_bytes)])
        else:
            resource_name = '/'.join(['uploads', self.u_uid, 'blobs',
                                      blob_digest.hash, str(blob_digest.size_bytes)])

        def __write_request_stream(resource, content):
            offset = 0
            finished = False
            remaining = len(content)
            while not finished:
                chunk_size = min(remaining, 64 * 1024)
                remaining -= chunk_size

                request = bytestream_pb2.WriteRequest()
                request.resource_name = resource
                request.data = content[offset:offset + chunk_size]
                request.write_offset = offset
                request.finish_write = remaining <= 0

                yield request

                offset += chunk_size
                finished = request.finish_write

        write_resquests = __write_request_stream(resource_name, blob)
        # TODO: Handle connection loss/recovery using QueryWriteStatus()
        write_response = self.__bytestream_stub.Write(write_resquests)

        assert write_response.committed_size == blob_digest.size_bytes

        return blob_digest

    def _send_batch(self):
        """Sends queued data using ContentAddressableStorage.BatchUpdateBlobs()"""
        batch_request = remote_execution_pb2.BatchUpdateBlobsRequest()
        batch_request.requests.extend(self.__requests.values())
        if self.instance_name is not None:
            batch_request.instance_name = self.instance_name

        batch_response = self.__cas_stub.BatchUpdateBlobs(batch_request)

        for response in batch_response.responses:
            assert response.digest.hash in self.__requests
            assert response.status.code is 0

        self.__requests.clear()
        self.__request_size = 0
