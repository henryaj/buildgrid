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
# pylint: disable=anomalous-backslash-in-string


from collections import namedtuple
from contextlib import contextmanager
import os
import uuid

import grpc

from buildgrid._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid._protos.google.rpc import code_pb2
from buildgrid.client.capabilities import CapabilitiesInterface
from buildgrid.settings import HASH, MAX_REQUEST_SIZE, MAX_REQUEST_COUNT, BATCH_REQUEST_SIZE_THRESHOLD
from buildgrid.utils import create_digest, merkle_tree_maker


_FileRequest = namedtuple('FileRequest', ['digest', 'output_paths'])


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


class _CasBatchRequestSizesCache:
    """Cache that stores, for each remote, the limit of bytes that can
    be transferred using batches as well as a threshold that determines
    when a file can be fetched as part of a batch request.
    """
    __cas_max_batch_transfer_size = {}
    __cas_batch_request_size_threshold = {}

    @classmethod
    def max_effective_batch_size_bytes(cls, channel, instance_name):
        """Returns the maximum number of bytes that can be transferred
        using batch methods for the given remote.
        """
        if channel not in cls.__cas_max_batch_transfer_size:
            cls.__cas_max_batch_transfer_size[channel] = dict()

        if instance_name not in cls.__cas_max_batch_transfer_size[channel]:
            max_batch_size = cls._get_server_max_batch_total_size_bytes(channel,
                                                                        instance_name)

            cls.__cas_max_batch_transfer_size[channel][instance_name] = max_batch_size

        return cls.__cas_max_batch_transfer_size[channel][instance_name]

    @classmethod
    def batch_request_size_threshold(cls, channel, instance_name):
        if channel not in cls.__cas_batch_request_size_threshold:
            cls.__cas_batch_request_size_threshold[channel] = dict()

        if instance_name not in cls.__cas_batch_request_size_threshold[channel]:
            # Computing the threshold:
            max_batch_size = cls.max_effective_batch_size_bytes(channel,
                                                                instance_name)
            threshold = BATCH_REQUEST_SIZE_THRESHOLD * max_batch_size

            cls.__cas_batch_request_size_threshold[channel][instance_name] = threshold

        return cls.__cas_batch_request_size_threshold[channel][instance_name]

    @classmethod
    def _get_server_max_batch_total_size_bytes(cls, channel, instance_name):
        """Returns the maximum number of bytes that can be effectively
        transferred using batches, considering the limits imposed by
        the server's configuration and by gRPC.
        """
        try:
            capabilities_interface = CapabilitiesInterface(channel)
            server_capabilities = capabilities_interface.get_capabilities(instance_name)

            cache_capabilities = server_capabilities.cache_capabilities

            max_batch_total_size = cache_capabilities.max_batch_total_size_bytes
            # The server could set this value to 0 (no limit set).
            if max_batch_total_size:
                return min(max_batch_total_size, MAX_REQUEST_SIZE)
        except ConnectionError:
            pass

        return MAX_REQUEST_SIZE


@contextmanager
def download(channel, instance=None, u_uid=None):
    """Context manager generator for the :class:`Downloader` class."""
    downloader = Downloader(channel, instance=instance)
    try:
        yield downloader
    finally:
        downloader.close()


class Downloader:
    """Remote CAS files, directories and messages download helper.

    The :class:`Downloader` class comes with a generator factory function that
    can be used together with the `with` statement for context management::

        from buildgrid.client.cas import download

        with download(channel, instance='build') as downloader:
            downloader.get_message(message_digest)
    """

    def __init__(self, channel, instance=None):
        """Initializes a new :class:`Downloader` instance.

        Args:
            channel (grpc.Channel): A gRPC channel to the CAS endpoint.
            instance (str, optional): the targeted instance's name.
        """
        self.channel = channel

        self.instance_name = instance

        self.__bytestream_stub = bytestream_pb2_grpc.ByteStreamStub(self.channel)
        self.__cas_stub = remote_execution_pb2_grpc.ContentAddressableStorageStub(self.channel)

        self.__file_requests = {}
        self.__file_request_count = 0
        self.__file_request_size = 0
        self.__file_response_size = 0

    # --- Public API ---

    def get_blob(self, digest):
        """Retrieves a blob from the remote CAS server.

        Args:
            digest (:obj:`Digest`): the blob's digest to fetch.

        Returns:
            bytearray: the fetched blob data or None if not found.
        """
        try:
            blob = self._fetch_blob(digest)
        except FileNotFoundError:
            return None
        except ConnectionError:
            raise

        return blob

    def get_blobs(self, digests):
        """Retrieves a list of blobs from the remote CAS server.

        Args:
            digests (list): list of :obj:`Digest`\ s for the blobs to fetch.

        Returns:
            list: the fetched blob data list.
        """
        return self._fetch_blob_batch(digests)

    def get_message(self, digest, message):
        """Retrieves a :obj:`Message` from the remote CAS server.

        Args:
            digest (:obj:`Digest`): the message's digest to fetch.
            message (:obj:`Message`): an empty message to fill.

        Returns:
            :obj:`Message`: `message` filled or emptied if not found.
        """
        try:
            message_blob = self._fetch_blob(digest)
        except NotFoundError:
            message_blob = None

        if message_blob is not None:
            message.ParseFromString(message_blob)
        else:
            message.Clear()

        return message

    def get_messages(self, digests, messages):
        """Retrieves a list of :obj:`Message`\ s from the remote CAS server.

        Note:
            The `digests` and `messages` list **must** contain the same number
            of elements.

        Args:
            digests (list):  list of :obj:`Digest`\ s for the messages to fetch.
            messages (list): list of empty :obj:`Message`\ s to fill.

        Returns:
            list: the fetched and filled message list.
        """
        assert len(digests) == len(messages)

        message_blobs = self._fetch_blob_batch(digests)

        assert len(message_blobs) == len(messages)

        for message, message_blob in zip(messages, message_blobs):
            message.ParseFromString(message_blob)

        return messages

    def download_file(self, digest, file_path, is_executable=False, queue=True):
        """Retrieves a file from the remote CAS server.

        If queuing is allowed (`queue=True`), the download request **may** be
        defer. An explicit call to :func:`~flush` can force the request to be
        send immediately (along with the rest of the queued batch).

        Args:
            digest (:obj:`Digest`): the file's digest to fetch.
            file_path (str): absolute or relative path to the local file to write.
            is_executable (bool): whether the file is executable or not.
            queue (bool, optional): whether or not the download request may be
                queued and submitted as part of a batch upload request. Defaults
                to True.

        Raises:
            FileNotFoundError: if `digest` is not present in the remote CAS server.
            OSError: if `file_path` does not exist or is not readable.
        """
        if not os.path.isabs(file_path):
            file_path = os.path.abspath(file_path)

        if not queue or digest.size_bytes > self._queueable_file_size_threshold():
            self._fetch_file(digest, file_path, is_executable=is_executable)
        else:
            self._queue_file(digest, file_path, is_executable=is_executable)

    def download_directory(self, digest, directory_path):
        """Retrieves a :obj:`Directory` from the remote CAS server.

        Args:
            digest (:obj:`Digest`): the directory's digest to fetch.

        Raises:
            NotFoundError: if `digest` is not present in the remote CAS server.
            FileExistsError: if `directory_path` already contains parts of their
                fetched directory's content.
        """
        if not os.path.isabs(directory_path):
            directory_path = os.path.abspath(directory_path)

        self._fetch_directory(digest, directory_path)

    def flush(self):
        """Ensures any queued request gets sent."""
        if self.__file_requests:
            self._fetch_file_batch(self.__file_requests)

            self.__file_requests.clear()
            self.__file_request_count = 0
            self.__file_request_size = 0
            self.__file_response_size = 0

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

    def _fetch_blob(self, digest):
        """Fetches a blob using ByteStream.Read()"""
        read_blob = bytearray()

        if self.instance_name:
            resource_name = '/'.join([self.instance_name, 'blobs',
                                      digest.hash, str(digest.size_bytes)])
        else:
            resource_name = '/'.join(['blobs', digest.hash, str(digest.size_bytes)])

        read_request = bytestream_pb2.ReadRequest()
        read_request.resource_name = resource_name
        read_request.read_offset = 0

        try:
            # TODO: Handle connection loss/recovery
            for read_response in self.__bytestream_stub.Read(read_request):
                read_blob += read_response.data

            assert len(read_blob) == digest.size_bytes

        except grpc.RpcError as e:
            status_code = e.code()
            if status_code == grpc.StatusCode.NOT_FOUND:
                raise FileNotFoundError("Requested data does not exist on the remote.")

            else:
                raise ConnectionError(e.details())

        return read_blob

    def _fetch_blob_batch(self, digests):
        """Fetches blobs using ContentAddressableStorage.BatchReadBlobs()"""
        batch_fetched = False
        read_blobs = []

        # First, try BatchReadBlobs(), if not already known not being implemented:
        if not _CallCache.unimplemented(self.channel, 'BatchReadBlobs'):
            batch_request = remote_execution_pb2.BatchReadBlobsRequest()
            batch_request.digests.extend(digests)
            if self.instance_name is not None:
                batch_request.instance_name = self.instance_name

            try:
                batch_response = self.__cas_stub.BatchReadBlobs(batch_request)
                for response in batch_response.responses:
                    assert response.digest in digests

                    read_blobs.append(response.data)

                    if response.status.code == code_pb2.NOT_FOUND:
                        raise FileNotFoundError('Requested blob does not exist '
                                                'on the remote.')
                    if response.status.code != code_pb2.OK:
                        raise ConnectionError('Error in CAS reply while fetching blob.')

                batch_fetched = True

            except grpc.RpcError as e:
                status_code = e.code()
                if status_code == grpc.StatusCode.UNIMPLEMENTED:
                    _CallCache.mark_unimplemented(self.channel, 'BatchReadBlobs')

                elif status_code == grpc.StatusCode.INVALID_ARGUMENT:
                    read_blobs.clear()
                    batch_fetched = False

                else:
                    raise ConnectionError(e.details())

        # Fallback to Read() if no BatchReadBlobs():
        if not batch_fetched:
            for digest in digests:
                read_blobs.append(self._fetch_blob(digest))

        return read_blobs

    def _fetch_file(self, digest, file_path, is_executable=False):
        """Fetches a file using ByteStream.Read()"""
        if self.instance_name:
            resource_name = '/'.join([self.instance_name, 'blobs',
                                      digest.hash, str(digest.size_bytes)])
        else:
            resource_name = '/'.join(['blobs', digest.hash, str(digest.size_bytes)])

        read_request = bytestream_pb2.ReadRequest()
        read_request.resource_name = resource_name
        read_request.read_offset = 0

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'wb') as byte_file:
            # TODO: Handle connection loss/recovery
            for read_response in self.__bytestream_stub.Read(read_request):
                byte_file.write(read_response.data)

            assert byte_file.tell() == digest.size_bytes

        if is_executable:
            os.chmod(file_path, 0o755)  # rwxr-xr-x / 755

    def _queue_file(self, digest, file_path, is_executable=False):
        """Queues a file for later batch download"""
        batch_size_limit = self._max_effective_batch_size_bytes()

        if self.__file_request_size + digest.ByteSize() > batch_size_limit:
            self.flush()
        elif self.__file_response_size + digest.size_bytes > batch_size_limit:
            self.flush()
        elif self.__file_request_count >= MAX_REQUEST_COUNT:
            self.flush()

        output_path = (file_path, is_executable)

        # When queueing a file we take into account the cases where
        # we might want to download the same digest to different paths.
        if digest.hash not in self.__file_requests:
            request = _FileRequest(digest=digest, output_paths=[output_path])
            self.__file_requests[digest.hash] = request

            self.__file_request_count += 1
            self.__file_request_size += digest.ByteSize()
            self.__file_response_size += digest.size_bytes
        else:
            # We already have that hash queued; we'll fetch the blob
            # once and write copies of it:
            self.__file_requests[digest.hash].output_paths.append(output_path)

    def _fetch_file_batch(self, requests):
        """Sends queued data using ContentAddressableStorage.BatchReadBlobs().

        Takes a dictionary (digest.hash, _FileRequest) as input.
        """
        batch_digests = [request.digest for request in requests.values()]
        batch_blobs = self._fetch_blob_batch(batch_digests)

        for file_digest, file_blob in zip(batch_digests, batch_blobs):
            output_paths = requests[file_digest.hash].output_paths

            for file_path, is_executable in output_paths:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)

                with open(file_path, 'wb') as byte_file:
                    byte_file.write(file_blob)

                if is_executable:
                    os.chmod(file_path, 0o755)  # rwxr-xr-x / 755

    def _fetch_directory(self, digest, directory_path):
        """Fetches a file using ByteStream.GetTree()"""
        # Better fail early if the local root path cannot be created:
        os.makedirs(directory_path, exist_ok=True)

        directories = {}
        directory_fetched = False
        # First, try GetTree() if not known to be unimplemented yet:
        if not _CallCache.unimplemented(self.channel, 'GetTree'):
            tree_request = remote_execution_pb2.GetTreeRequest()
            tree_request.root_digest.CopyFrom(digest)
            tree_request.page_size = MAX_REQUEST_COUNT
            if self.instance_name is not None:
                tree_request.instance_name = self.instance_name

            try:
                for tree_response in self.__cas_stub.GetTree(tree_request):
                    for directory in tree_response.directories:
                        directory_blob = directory.SerializeToString()
                        directory_hash = HASH(directory_blob).hexdigest()

                        directories[directory_hash] = directory

                assert digest.hash in directories

                directory = directories[digest.hash]
                self._write_directory(directory, directory_path,
                                      directories=directories, root_barrier=directory_path)

                directory_fetched = True
            except grpc.RpcError as e:
                status_code = e.code()
                if status_code == grpc.StatusCode.UNIMPLEMENTED:
                    _CallCache.mark_unimplemented(self.channel, 'GetTree')

                elif status_code == grpc.StatusCode.NOT_FOUND:
                    raise FileNotFoundError("Requested directory does not exist on the remote.")

                else:
                    raise ConnectionError(e.details())

        # If no GetTree(), _write_directory() will use BatchReadBlobs()
        # if available or Read() if not.
        if not directory_fetched:
            directory = remote_execution_pb2.Directory()
            directory.ParseFromString(self._fetch_blob(digest))

            self._write_directory(directory, directory_path,
                                  root_barrier=directory_path)

    def _write_directory(self, root_directory, root_path, directories=None,
                         root_barrier=None):
        """Generates a local directory structure"""

        # i) Files:
        for file_node in root_directory.files:
            file_path = os.path.join(root_path, file_node.name)

            self.download_file(file_node.digest, file_path,
                               is_executable=file_node.is_executable)
        self.flush()

        # ii) Directories:
        pending_directory_digests = []
        pending_directory_paths = {}
        for directory_node in root_directory.directories:
            directory_hash = directory_node.digest.hash

            directory_path = os.path.join(root_path, directory_node.name)
            os.makedirs(directory_path, exist_ok=True)

            if directories and directory_node.digest.hash in directories:
                # We already have the directory; just write it:
                directory = directories[directory_hash]

                self._write_directory(directory, directory_path,
                                      directories=directories,
                                      root_barrier=root_barrier)
            else:
                # Gather all the directories that we need to get to
                # try fetching them in a single batch request:
                pending_directory_digests.append(directory_node.digest)
                pending_directory_paths[directory_hash] = directory_path

        if pending_directory_paths:
            fetched_blobs = self._fetch_blob_batch(pending_directory_digests)

            for directory_blob in fetched_blobs:
                directory = remote_execution_pb2.Directory()
                directory.ParseFromString(directory_blob)

                # Assuming that the server might not return the blobs in
                # the same order than they were asked for, we read
                # the hashes of the returned blobs:
                directory_hash = create_digest(directory_blob).hash
                # Guarantees for the reply orderings might change in
                # the specification at some point.
                # See: github.com/bazelbuild/remote-apis/issues/52

                directory_path = pending_directory_paths[directory_hash]

                self._write_directory(directory, directory_path,
                                      directories=directories,
                                      root_barrier=root_barrier)

        # iii) Symlinks:
        for symlink_node in root_directory.symlinks:
            symlink_path = os.path.join(root_path, symlink_node.name)
            if not os.path.isabs(symlink_node.target):
                target_path = os.path.join(root_path, symlink_node.target)
            else:
                target_path = symlink_node.target
            target_path = os.path.normpath(target_path)

            # Do not create links pointing outside the barrier:
            if root_barrier is not None:
                common_path = os.path.commonprefix([root_barrier, target_path])
                if not common_path.startswith(root_barrier):
                    continue

            os.symlink(symlink_path, target_path)

    def _max_effective_batch_size_bytes(self):
        """Returns the effective maximum number of bytes that can be
        transferred using batches, considering gRPC maximum message size.
        """
        return _CasBatchRequestSizesCache.max_effective_batch_size_bytes(self.channel,
                                                                         self.instance_name)

    def _queueable_file_size_threshold(self):
        """Returns the size limit up until which files can be queued to
        be requested in a batch.
        """
        return _CasBatchRequestSizesCache.batch_request_size_threshold(self.channel,
                                                                       self.instance_name)


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
    """

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

        if not queue or len(blob) > self._queueable_file_size_threshold():
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

        if not queue or len(message_blob) > self._queueable_file_size_threshold():
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

        if not queue or len(file_bytes) > self._queueable_file_size_threshold():
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
        if self.instance_name:
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
                chunk_size = min(remaining, MAX_REQUEST_SIZE)
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
        try:
            write_response = self.__bytestream_stub.Write(write_resquests)
        except grpc.RpcError as e:
            raise ConnectionError(e.details())

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

        # If we are here queueing a file we know that its size is
        # smaller than gRPC's message size limit.
        # We'll make a single batch request as big as the server allows.
        batch_size_limit = self._max_effective_batch_size_bytes()

        if self.__request_size + blob_digest.size_bytes > batch_size_limit:
            self.flush()
        elif self.__request_count >= MAX_REQUEST_COUNT:
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
                    raise ConnectionError(e.details())

        # Fallback to Write() if no BatchUpdateBlobs():
        if not batch_fetched:
            for blob, digest in batch.values():
                written_digests.append(self._send_blob(blob, digest=digest))

        return written_digests

    def _max_effective_batch_size_bytes(self):
        """Returns the effective maximum number of bytes that can be
        transferred using batches, considering gRPC maximum message size.
        """
        return _CasBatchRequestSizesCache.max_effective_batch_size_bytes(self.channel,
                                                                         self.instance_name)

    def _queueable_file_size_threshold(self):
        """Returns the size limit up until which files can be queued to
        be requested in a batch.
        """
        return _CasBatchRequestSizesCache.batch_request_size_threshold(self.channel,
                                                                       self.instance_name)
