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

import grpc

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid._protos.google.rpc import code_pb2
from buildgrid.settings import HASH
from buildgrid.utils import merkle_tree_maker


class _CallCache:
    """Per remote grpc.StatusCode.UNIMPLEMENTED call cache."""
    __calls = {}

    @classmethod
    def mark_unimplemented(cls, channel, name):
        if channel not in cls.__calls:
            cls.__calls[channel] = set()
        cls.__calls[channel].add(name)

    @classmethod
    def unimplemented(cls, channel, name):
        if channel not in cls.__calls:
            return False
        return name in cls.__calls[channel]


@contextmanager
def upload(channel, instance=None, u_uid=None):
    """Context manager generator for the :class:`Uploader` class."""
    uploader = Uploader(channel, instance=instance, u_uid=u_uid)
    try:
        yield uploader
    finally:
        uploader.close()


class Uploader:
    """Remote CAS files, directories and messages upload helper.

    The :class:`Uploader` class comes with a generator factory function that can
    be used together with the `with` statement for context management::

        from buildgrid.client.cas import upload

        with upload(channel, instance='build') as uploader:
            uploader.upload_file('/path/to/local/file')

    Attributes:
        FILE_SIZE_THRESHOLD (int): maximum size for a queueable file.
        MAX_REQUEST_SIZE (int): maximum size for a single gRPC request.
    """

    FILE_SIZE_THRESHOLD = 1 * 1024 * 1024
    MAX_REQUEST_SIZE = 2 * 1024 * 1024
    MAX_REQUEST_COUNT = 500

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

        self.__requests = {}
        self.__request_count = 0
        self.__request_size = 0

    # --- Public API ---

    def put_blob(self, blob, digest=None, queue=False):
        """Stores a blob into the remote CAS server.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (along with the rest of the queued batch).

        Args:
            blob (bytes): the blob's data.
            digest (:obj:`Digest`, optional): the blob's digest.
            queue (bool, optional): whether or not the upload request may be
                queued and submitted as part of a batch upload request. Defaults
                to False.

        Returns:
            :obj:`Digest`: the sent blob's digest.
        """
        if not queue or len(blob) > Uploader.FILE_SIZE_THRESHOLD:
            blob_digest = self._send_blob(blob, digest=digest)
        else:
            blob_digest = self._queue_blob(blob, digest=digest)

        return blob_digest

    def put_message(self, message, digest=None, queue=False):
        """Stores a message into the remote CAS server.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (along with the rest of the queued batch).

        Args:
            message (:obj:`Message`): the message object.
            digest (:obj:`Digest`, optional): the message's digest.
            queue (bool, optional): whether or not the upload request may be
                queued and submitted as part of a batch upload request. Defaults
                to False.

        Returns:
            :obj:`Digest`: the sent message's digest.
        """
        message_blob = message.SerializeToString()

        if not queue or len(message_blob) > Uploader.FILE_SIZE_THRESHOLD:
            message_digest = self._send_blob(message_blob, digest=digest)
        else:
            message_digest = self._queue_blob(message_blob, digest=digest)

        return message_digest

    def upload_file(self, file_path, queue=True):
        """Stores a local file into the remote CAS storage.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (allong with the rest of the queued batch).

        Args:
            file_path (str): absolute or relative path to a local file.
            queue (bool, optional): whether or not the upload request may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Returns:
            :obj:`Digest`: The digest of the file's content.

        Raises:
            FileNotFoundError: If `file_path` does not exist.
            PermissionError: If `file_path` is not readable.
        """
        if not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)

        with open(file_path, 'rb') as bytes_steam:
            file_bytes = bytes_steam.read()

        if not queue or len(file_bytes) > Uploader.FILE_SIZE_THRESHOLD:
            file_digest = self._send_blob(file_bytes)
        else:
            file_digest = self._queue_blob(file_bytes)

        return file_digest

    def upload_directory(self, directory_path, queue=True):
        """Stores a local folder into the remote CAS storage.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (allong with the rest of the queued batch).

        Args:
            directory_path (str): absolute or relative path to a local folder.
            queue (bool, optional): wheter or not the upload requests may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Returns:
            :obj:`Digest`: The digest of the top :obj:`Directory`.

        Raises:
            FileNotFoundError: If `directory_path` does not exist.
            PermissionError: If `directory_path` is not readable.
        """
        if not os.path.isabs(directory_path):
            directory_path = os.path.abspath(directory_path)

        last_directory_node = None

        if not queue:
            for node, blob, _ in merkle_tree_maker(directory_path):
                if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                    last_directory_node = node

                self._send_blob(blob, digest=node.digest)

        else:
            for node, blob, _ in merkle_tree_maker(directory_path):
                if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                    last_directory_node = node

                self._queue_blob(blob, digest=node.digest)

        return last_directory_node.digest

    def upload_tree(self, directory_path, queue=True):
        """Stores a local folder into the remote CAS storage as a :obj:`Tree`.

        If queuing is allowed (`queue=True`), the upload request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (allong with the rest of the queued batch).

        Args:
            directory_path (str): absolute or relative path to a local folder.
            queue (bool, optional): wheter or not the upload requests may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Returns:
            :obj:`Digest`: The digest of the :obj:`Tree`.

        Raises:
            FileNotFoundError: If `directory_path` does not exist.
            PermissionError: If `directory_path` is not readable.
        """
        if not os.path.isabs(directory_path):
            directory_path = os.path.abspath(directory_path)

        directories = []

        if not queue:
            for node, blob, _ in merkle_tree_maker(directory_path):
                if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                    # TODO: Get the Directory object from merkle_tree_maker():
                    directory = remote_execution_pb2.Directory()
                    directory.ParseFromString(blob)
                    directories.append(directory)

                self._send_blob(blob, digest=node.digest)

        else:
            for node, blob, _ in merkle_tree_maker(directory_path):
                if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                    # TODO: Get the Directory object from merkle_tree_maker():
                    directory = remote_execution_pb2.Directory()
                    directory.ParseFromString(blob)
                    directories.append(directory)

                self._queue_blob(blob, digest=node.digest)

        tree = remote_execution_pb2.Tree()
        tree.root.CopyFrom(directories[-1])
        tree.children.extend(directories[:-1])

        return self.put_message(tree, queue=queue)

    def flush(self):
        """Ensures any queued request gets sent."""
        if self.__requests:
            self._send_blob_batch(self.__requests)

            self.__requests.clear()
            self.__request_count = 0
            self.__request_size = 0

    def close(self):
        """Closes the underlying connection stubs.

        Note:
            This will always send pending requests before closing connections,
            if any.
        """
        self.flush()

        self.__bytestream_stub = None
        self.__cas_stub = None

    # --- Private API ---

    def _send_blob(self, blob, digest=None):
        """Sends a memory block using ByteStream.Write()"""
        blob_digest = remote_execution_pb2.Digest()
        if digest is not None:
            blob_digest.CopyFrom(digest)
        else:
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
                chunk_size = min(remaining, Uploader.MAX_REQUEST_SIZE)
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

    def _queue_blob(self, blob, digest=None):
        """Queues a memory block for later batch upload"""
        blob_digest = remote_execution_pb2.Digest()
        if digest is not None:
            blob_digest.CopyFrom(digest)
        else:
            blob_digest.hash = HASH(blob).hexdigest()
            blob_digest.size_bytes = len(blob)

        if self.__request_size + blob_digest.size_bytes > Uploader.MAX_REQUEST_SIZE:
            self.flush()
        elif self.__request_count >= Uploader.MAX_REQUEST_COUNT:
            self.flush()

        self.__requests[blob_digest.hash] = (blob, blob_digest)
        self.__request_count += 1
        self.__request_size += blob_digest.size_bytes

        return blob_digest

    def _send_blob_batch(self, batch):
        """Sends queued data using ContentAddressableStorage.BatchUpdateBlobs()"""
        batch_fetched = False
        written_digests = []

        # First, try BatchUpdateBlobs(), if not already known not being implemented:
        if not _CallCache.unimplemented(self.channel, 'BatchUpdateBlobs'):
            batch_request = remote_execution_pb2.BatchUpdateBlobsRequest()
            if self.instance_name is not None:
                batch_request.instance_name = self.instance_name

            for blob, digest in batch.values():
                request = batch_request.requests.add()
                request.digest.CopyFrom(digest)
                request.data = blob

            try:
                batch_response = self.__cas_stub.BatchUpdateBlobs(batch_request)
                for response in batch_response.responses:
                    assert response.digest.hash in batch

                    written_digests.append(response.digest)
                    if response.status.code != code_pb2.OK:
                        response.digest.Clear()

                batch_fetched = True

            except grpc.RpcError as e:
                status_code = e.code()
                if status_code == grpc.StatusCode.UNIMPLEMENTED:
                    _CallCache.mark_unimplemented(self.channel, 'BatchUpdateBlobs')

                elif status_code == grpc.StatusCode.INVALID_ARGUMENT:
                    written_digests.clear()
                    batch_fetched = False

                else:
                    assert False

        # Fallback to Write() if no BatchUpdateBlobs():
        if not batch_fetched:
            for blob, digest in batch.values():
                written_digests.append(self._send_blob(blob, digest=digest))

        return written_digests
