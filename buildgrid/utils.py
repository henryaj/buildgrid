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


from urllib.parse import urljoin
from operator import attrgetter
import os
import socket
import threading

from buildgrid.settings import HASH, HASH_LENGTH, BROWSER_URL_FORMAT
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2


class BrowserURL:

    __url_markers = (
        '%(instance)s',
        '%(type)s',
        '%(hash)s',
        '%(sizebytes)s',
    )

    def __init__(self, base_url, instance_name=None):
        """Begins browser URL helper initialization."""
        self.__base_url = base_url
        self.__initialized = False
        self.__url_spec = {
            '%(instance)s': instance_name or '',
        }

    def for_message(self, message_type, message_digest):
        """Completes browser URL initialization for a protobuf message."""
        if self.__initialized:
            return False

        self.__url_spec['%(type)s'] = message_type
        self.__url_spec['%(hash)s'] = message_digest.hash
        self.__url_spec['%(sizebytes)s'] = str(message_digest.size_bytes)

        self.__initialized = True
        return True

    def generate(self):
        """Generates a browser URL string."""
        if not self.__base_url or not self.__initialized:
            return None

        url_tail = BROWSER_URL_FORMAT

        for url_marker in self.__url_markers:
            if url_marker not in self.__url_spec:
                return None
            if url_marker not in url_tail:
                continue
            url_tail = url_tail.replace(url_marker, self.__url_spec[url_marker])

        return urljoin(self.__base_url, url_tail)


class Condition(threading.Condition):

    """Subclass of ``threading.Condition`` with ``wait`` overridden.

    In this implementation, ``wait`` only releases the lock if other
    threads are actually waiting for the lock, otherwise it does nothing.

    """

    def __init__(self, lock=None):
        super().__init__(lock=lock)
        self.thread_count = 0

    def __enter__(self):
        self.thread_count += 1
        return super().__enter__()

    def __exit__(self, *args):
        self.thread_count -= 1
        return super().__exit__(*args)

    def wait(self, timeout=None):
        """Wait if other threads are trying to acquire the lock.

        If other threads have attempted to acquire the lock for this Condition
        using ``with``, this method behaves the same as ``wait`` on a regular
        ``threading.Condition``.

        If only one thread has attempted to acquire the lock, then that thread
        must be the current thread. In that case, this method doesn't release
        the lock or wait at all, it simply returns ``True`` as if it had been
        woken by ``threading.Condition.notify``.

        """
        if not super()._is_owned:
            raise RuntimeError("cannot wait on un-acquired lock")

        if self.thread_count > 1:
            return super().wait(timeout)
        return True


def get_hostname():
    """Returns the hostname of the machine executing that function.

    Returns:
        str: Hostname for the current machine.
    """
    return socket.gethostname()


def get_hash_type():
    """Returns the hash type."""
    hash_name = HASH().name
    if hash_name == "sha256":
        return remote_execution_pb2.DigestFunction.SHA256
    return remote_execution_pb2.DigestFunction.UNKNOWN


def create_digest(bytes_to_digest):
    """Computes the :obj:`Digest` of a piece of data.

    The :obj:`Digest` of a data is a function of its hash **and** size.

    Args:
        bytes_to_digest (bytes): byte data to digest.

    Returns:
        :obj:`Digest`: The :obj:`Digest` for the given byte data.
    """
    return remote_execution_pb2.Digest(hash=HASH(bytes_to_digest).hexdigest(),
                                       size_bytes=len(bytes_to_digest))


def parse_digest(digest_string):
    """Creates a :obj:`Digest` from a digest string.

    A digest string should alway be: ``{hash}/{size_bytes}``.

    Args:
        digest_string (str): the digest string.

    Returns:
        :obj:`Digest`: The :obj:`Digest` read from the string or None if
            `digest_string` is not a valid digest string.
    """
    digest_hash, digest_size = digest_string.split('/')

    if len(digest_hash) == HASH_LENGTH and digest_size.isdigit():
        return remote_execution_pb2.Digest(hash=digest_hash,
                                           size_bytes=int(digest_size))

    return None


def read_file(file_path):
    """Loads raw file content in memory.

    Args:
        file_path (str): path to the target file.

    Returns:
        bytes: Raw file's content until EOF.

    Raises:
        OSError: If `file_path` does not exist or is not readable.
    """
    with open(file_path, 'rb') as byte_file:
        return byte_file.read()


def write_file(file_path, content):
    """Dumps raw memory content to a file.

    Args:
        file_path (str): path to the target file.
        content (bytes): raw file's content.

    Raises:
        OSError: If `file_path` does not exist or is not writable.
    """
    with open(file_path, 'wb') as byte_file:
        byte_file.write(content)
        byte_file.flush()


def merkle_tree_maker(directory_path):
    """Walks a local folder tree, generating :obj:`FileNode` and
    :obj:`DirectoryNode`.

    Args:
        directory_path (str): absolute or relative path to a local directory.

    Yields:
        :obj:`Message`, bytes, str: a tutple of either a :obj:`FileNode` or
        :obj:`DirectoryNode` message, the corresponding blob and the
        corresponding node path.
    """
    directory_name = os.path.basename(directory_path)

    # Actual generator, yields recursively FileNodes and DirectoryNodes:
    def __merkle_tree_maker(directory_path, directory_name):
        if not os.path.isabs(directory_path):
            directory_path = os.path.abspath(directory_path)

        directory = remote_execution_pb2.Directory()

        files, directories, symlinks = [], [], []
        for directory_entry in os.scandir(directory_path):
            node_name, node_path = directory_entry.name, directory_entry.path

            if directory_entry.is_file(follow_symlinks=False):
                node_blob = read_file(directory_entry.path)
                node_digest = create_digest(node_blob)

                node = remote_execution_pb2.FileNode()
                node.name = node_name
                node.digest.CopyFrom(node_digest)
                node.is_executable = os.access(node_path, os.X_OK)

                files.append(node)

                yield node, node_blob, node_path

            elif directory_entry.is_dir(follow_symlinks=False):
                node, node_blob, _ = yield from __merkle_tree_maker(node_path, node_name)

                directories.append(node)

                yield node, node_blob, node_path

            # Create a SymlinkNode;
            elif os.path.islink(directory_entry.path):
                node_target = os.readlink(directory_entry.path)

                node = remote_execution_pb2.SymlinkNode()
                node.name = directory_entry.name
                node.target = node_target

                symlinks.append(node)

        files.sort(key=attrgetter('name'))
        directories.sort(key=attrgetter('name'))
        symlinks.sort(key=attrgetter('name'))

        directory.files.extend(files)
        directory.directories.extend(directories)
        directory.symlinks.extend(symlinks)

        node_blob = directory.SerializeToString()
        node_digest = create_digest(node_blob)

        node = remote_execution_pb2.DirectoryNode()
        node.name = directory_name
        node.digest.CopyFrom(node_digest)

        return node, node_blob, directory_path

    node, node_blob, node_path = yield from __merkle_tree_maker(directory_path,
                                                                directory_name)

    yield node, node_blob, node_path


def output_file_maker(file_path, input_path, file_digest):
    """Creates an :obj:`OutputFile` from a local file and possibly upload it.

    Note:
        `file_path` **must** point inside or be relative to `input_path`.

    Args:
        file_path (str): absolute or relative path to a local file.
        input_path (str): absolute or relative path to the input root directory.
        file_digest (:obj:`Digest`): the underlying file's digest.

    Returns:
        :obj:`OutputFile`: a new :obj:`OutputFile` object for the file pointed
        by `file_path`.
    """
    if not os.path.isabs(file_path):
        file_path = os.path.abspath(file_path)
    if not os.path.isabs(input_path):
        input_path = os.path.abspath(input_path)

    output_file = remote_execution_pb2.OutputFile()
    output_file.digest.CopyFrom(file_digest)
    # OutputFile.path should be relative to the working directory
    output_file.path = os.path.relpath(file_path, start=input_path)
    output_file.is_executable = os.access(file_path, os.X_OK)

    return output_file


def output_directory_maker(directory_path, working_path, tree_digest):
    """Creates an :obj:`OutputDirectory` from a local directory.

    Note:
        `directory_path` **must** point inside or be relative to `input_path`.

    Args:
        directory_path (str): absolute or relative path to a local directory.
        working_path (str): absolute or relative path to the working directory.
        tree_digest (:obj:`Digest`): the underlying folder tree's digest.

    Returns:
        :obj:`OutputDirectory`: a new :obj:`OutputDirectory` for the directory
        pointed by `directory_path`.
    """
    if not os.path.isabs(directory_path):
        directory_path = os.path.abspath(directory_path)
    if not os.path.isabs(working_path):
        working_path = os.path.abspath(working_path)

    output_directory = remote_execution_pb2.OutputDirectory()
    output_directory.tree_digest.CopyFrom(tree_digest)
    output_directory.path = os.path.relpath(directory_path, start=working_path)

    return output_directory
