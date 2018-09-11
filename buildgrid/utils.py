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
        :obj:`Digest`: The gRPC :obj:`Digest` for the given byte data.
    """
    return remote_execution_pb2.Digest(hash=HASH(bytes_to_digest).hexdigest(),
                                       size_bytes=len(bytes_to_digest))


def merkle_maker(directory):
    """ Walks thorugh given directory, yielding the binary and digest
    """
    directory_pb2 = remote_execution_pb2.Directory()
    for (dir_path, dir_names, file_names) in os.walk(directory):

        for file_name in file_names:
            file_path = os.path.join(dir_path, file_name)
            chunk = read_file(file_path)
            file_digest = create_digest(chunk)
            directory_pb2.files.extend([file_maker(file_path, file_digest)])
            yield chunk, file_digest

        for inner_dir in dir_names:
            inner_dir_path = os.path.join(dir_path, inner_dir)
            yield from merkle_maker(inner_dir_path)

    directory_string = directory_pb2.SerializeToString()

    yield directory_string, create_digest(directory_string)


def file_maker(file_path, file_digest):
    """ Creates a File Node
    """
    _, file_name = os.path.split(file_path)
    return remote_execution_pb2.FileNode(name=file_name,
                                         digest=file_digest,
                                         is_executable=os.access(file_path, os.X_OK))


def directory_maker(directory_path, child_directories=None, cas=None, upload_directories=True):
    """Creates a :obj:`Directory` from a local directory and possibly upload it.

    Args:
        directory_path (str): absolute or relative path to a local directory.
        child_directories (list): output list of of children :obj:`Directory`
            objects.
        cas (:obj:`Uploader`): a CAS client uploader.
        upload_directories (bool): wheter or not to upload the :obj:`Directory`
            objects along with the files.

    Returns:
        :obj:`Directory`, :obj:`Digest`: Tuple of a new gRPC :obj:`Directory`
        for the local directory pointed by `directory_path` and the digest
        for that object.
    """
    if not os.path.isabs(directory_path):
        directory_path = os.path.abspath(directory_path)

    files, directories, symlinks = list(), list(), list()
    for directory_entry in os.scandir(directory_path):
        # Create a FileNode and corresponding BatchUpdateBlobsRequest:
        if directory_entry.is_file(follow_symlinks=False):
            if cas is not None:
                node_digest = cas.upload_file(directory_entry.path)
            else:
                node_digest = create_digest(read_file(directory_entry.path))

            node = remote_execution_pb2.FileNode()
            node.name = directory_entry.name
            node.digest.CopyFrom(node_digest)
            node.is_executable = os.access(directory_entry.path, os.X_OK)

            files.append(node)

        # Create a DirectoryNode and corresponding BatchUpdateBlobsRequest:
        elif directory_entry.is_dir(follow_symlinks=False):
            _, node_digest = directory_maker(directory_entry.path,
                                             child_directories=child_directories,
                                             upload_directories=upload_directories,
                                             cas=cas)

            node = remote_execution_pb2.DirectoryNode()
            node.name = directory_entry.name
            node.digest.CopyFrom(node_digest)

            directories.append(node)

        # Create a SymlinkNode if necessary;
        elif os.path.islink(directory_entry.path):
            node_target = os.readlink(directory_entry.path)

            node = remote_execution_pb2.SymlinkNode()
            node.name = directory_entry.name
            node.target = node_target

            symlinks.append(node)

    files.sort(key=attrgetter('name'))
    directories.sort(key=attrgetter('name'))
    symlinks.sort(key=attrgetter('name'))

    directory = remote_execution_pb2.Directory()
    directory.files.extend(files)
    directory.directories.extend(directories)
    directory.symlinks.extend(symlinks)

    if child_directories is not None:
        child_directories.append(directory)

    if cas is not None and upload_directories:
        directory_digest = cas.upload_directory(directory)
    else:
        directory_digest = create_digest(directory.SerializeToString())

    return directory, directory_digest


def tree_maker(directory_path, cas=None):
    """Creates a :obj:`Tree` from a local directory and possibly upload it.

    If `cas` is specified, the local directory content will be uploded/stored
    in remote CAS (the :obj:`Tree` message won't).

    Args:
        directory_path (str): absolute or relative path to a local directory.
        cas (:obj:`Uploader`): a CAS client uploader.

    Returns:
        :obj:`Tree`, :obj:`Digest`: Tuple of a new gRPC :obj:`Tree` for the
        local directory pointed by `directory_path` and the digest for that
        object.
    """
    if not os.path.isabs(directory_path):
        directory_path = os.path.abspath(directory_path)

    child_directories = list()
    directory, _ = directory_maker(directory_path,
                                   child_directories=child_directories,
                                   upload_directories=False,
                                   cas=cas)

    tree = remote_execution_pb2.Tree()
    tree.children.extend(child_directories)
    tree.root.CopyFrom(directory)

    # Ensure that we've uploded the tree structure first
    if cas is not None:
        cas.flush()
        tree_digest = cas.put_message(tree)
    else:
        tree_digest = create_digest(tree.SerializeToString())

    return tree, tree_digest


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


def output_file_maker(file_path, input_path, cas=None):
    """Creates an :obj:`OutputFile` from a local file and possibly upload it.

    If `cas` is specified, the local file will be uploded/stored in remote CAS
    (the :obj:`OutputFile` message won't).

    Note:
        `file_path` **must** point inside or be relative to `input_path`.

    Args:
        file_path (str): absolute or relative path to a local file.
        input_path (str): absolute or relative path to the input root directory.
        cas (:obj:`Uploader`): a CAS client uploader.

    Returns:
        :obj:`OutputFile`: a new gRPC :obj:`OutputFile` object for the file
        pointed by `file_path`.
    """
    if not os.path.isabs(file_path):
        file_path = os.path.abspath(file_path)
    if not os.path.isabs(input_path):
        input_path = os.path.abspath(input_path)

    if cas is not None:
        file_digest = cas.upload_file(file_path)
    else:
        file_digest = create_digest(read_file(file_path))

    output_file = remote_execution_pb2.OutputFile()
    output_file.digest.CopyFrom(file_digest)
    # OutputFile.path should be relative to the working direcory:
    output_file.path = os.path.relpath(file_path, start=input_path)
    output_file.is_executable = os.access(file_path, os.X_OK)

    return output_file


def output_directory_maker(directory_path, working_path, cas=None):
    """Creates an :obj:`OutputDirectory` from a local directory.

    If `cas` is specified, the local directory content will be uploded/stored
    in remote CAS (the :obj:`OutputDirectory` message won't).

    Note:
        `directory_path` **must** point inside or be relative to `input_path`.

    Args:
        directory_path (str): absolute or relative path to a local directory.
        working_path (str): absolute or relative path to the working directory.
        cas (:obj:`Uploader`): a CAS client uploader.

    Returns:
        :obj:`OutputDirectory`: a new gRPC :obj:`OutputDirectory` for the
        directory pointed by `directory_path`.
    """
    if not os.path.isabs(directory_path):
        directory_path = os.path.abspath(directory_path)
    if not os.path.isabs(working_path):
        working_path = os.path.abspath(working_path)

    _, tree_digest = tree_maker(directory_path, cas=cas)

    output_directory = remote_execution_pb2.OutputDirectory()
    output_directory.tree_digest.CopyFrom(tree_digest)
    output_directory.path = os.path.relpath(directory_path, start=working_path)

    return output_directory
