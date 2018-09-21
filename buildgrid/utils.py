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


from operator import attrgetter
import os
import uuid

from buildgrid.settings import HASH
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.bytestream import bytestream_pb2


def gen_fetch_blob(stub, digest, instance_name=""):
    """ Generates byte stream from a fetch blob request
    """

    resource_name = os.path.join(instance_name, 'blobs', digest.hash, str(digest.size_bytes))
    request = bytestream_pb2.ReadRequest(resource_name=resource_name,
                                         read_offset=0)

    for response in stub.Read(request):
        yield response.data


def gen_write_request_blob(digest_bytes, digest, instance_name=""):
    """ Generates a bytestream write request
    """
    resource_name = os.path.join(instance_name, 'uploads', str(uuid.uuid4()),
                                 'blobs', digest.hash, str(digest.size_bytes))

    offset = 0
    finished = False
    remaining = digest.size_bytes

    while not finished:
        chunk_size = min(remaining, 64 * 1024)
        remaining -= chunk_size
        finished = remaining <= 0

        request = bytestream_pb2.WriteRequest()
        request.resource_name = resource_name
        request.write_offset = offset
        request.data = digest_bytes.read(chunk_size)
        request.finish_write = finished

        yield request

        offset += chunk_size


def write_fetch_directory(root_directory, stub, digest, instance_name=None):
    """Locally replicates a directory from CAS.

    Args:
        root_directory (str): local directory to populate.
        stub (): gRPC stub for CAS communication.
        digest (Digest): digest for the directory to fetch from CAS.
        instance_name (str, optional): farm instance name to query data from.
    """

    if not os.path.isabs(root_directory):
        root_directory = os.path.abspath(root_directory)
    if not os.path.exists(root_directory):
        os.makedirs(root_directory, exist_ok=True)

    directory = parse_to_pb2_from_fetch(remote_execution_pb2.Directory(),
                                        stub, digest, instance_name)

    for directory_node in directory.directories:
        child_path = os.path.join(root_directory, directory_node.name)

        write_fetch_directory(child_path, stub, directory_node.digest, instance_name)

    for file_node in directory.files:
        child_path = os.path.join(root_directory, file_node.name)

        with open(child_path, 'wb') as child_file:
            write_fetch_blob(child_file, stub, file_node.digest, instance_name)

    for symlink_node in directory.symlinks:
        child_path = os.path.join(root_directory, symlink_node.name)

        if os.path.isabs(symlink_node.target):
            continue  # No out of temp-directory links for now.
        target_path = os.path.join(root_directory, symlink_node.target)

        os.symlink(child_path, target_path)


def write_fetch_blob(target_file, stub, digest, instance_name=None):
    """Extracts a blob from CAS into a local file.

    Args:
        target_file (str): local file to write.
        stub (): gRPC stub for CAS communication.
        digest (Digest): digest for the blob to fetch from CAS.
        instance_name (str, optional): farm instance name to query data from.
    """

    for stream in gen_fetch_blob(stub, digest, instance_name):
        target_file.write(stream)
    target_file.flush()

    assert digest.size_bytes == os.fstat(target_file.fileno()).st_size


def parse_to_pb2_from_fetch(pb2, stub, digest, instance_name=""):
    """ Fetches stream and parses it into given pb2
    """

    stream_bytes = b''
    for stream in gen_fetch_blob(stub, digest, instance_name):
        stream_bytes += stream

    pb2.ParseFromString(stream_bytes)
    return pb2


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
