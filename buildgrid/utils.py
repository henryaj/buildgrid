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

import os

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


def write_fetch_directory(directory, stub, digest, instance_name=""):
    """ Given a directory digest, fetches files and writes them to a directory
    """
    # TODO: Extend to symlinks and inner directories
    # pathlib.Path('/my/directory').mkdir(parents=True, exist_ok=True)

    directory_pb2 = remote_execution_pb2.Directory()
    directory_pb2 = parse_to_pb2_from_fetch(directory_pb2, stub, digest, instance_name)

    for file_node in directory_pb2.files:
        path = os.path.join(directory, file_node.name)
        with open(path, 'wb') as f:
            write_fetch_blob(f, stub, file_node.digest, instance_name)


def write_fetch_blob(out, stub, digest, instance_name=""):
    """ Given an output buffer, fetches blob and writes to buffer
    """

    for stream in gen_fetch_blob(stub, digest, instance_name):
        out.write(stream)

    out.flush()
    assert digest.size_bytes == os.fstat(out.fileno()).st_size


def parse_to_pb2_from_fetch(pb2, stub, digest, instance_name=""):
    """ Fetches stream and parses it into given pb2
    """

    stream_bytes = b''
    for stream in gen_fetch_blob(stub, digest, instance_name):
        stream_bytes += stream

    pb2.ParseFromString(stream_bytes)
    return pb2


def create_digest(bytes_to_digest):
    """ Creates a hash based on the hex digest and returns the digest
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


def read_file(read):
    with open(read, 'rb') as f:
        return f.read()
