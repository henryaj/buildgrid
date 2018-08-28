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
import subprocess
import tempfile

from google.protobuf import any_pb2

from buildgrid.utils import read_file, create_digest, write_fetch_directory, parse_to_pb2_from_fetch
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.bytestream import bytestream_pb2_grpc


def work_temp_directory(context, lease):
    """ Bot downloads directories and files into a temp directory,
    then uploads results back to CAS
    """

    parent = context.parent
    stub_bytestream = bytestream_pb2_grpc.ByteStreamStub(context.cas_channel)

    action_digest = remote_execution_pb2.Digest()
    lease.payload.Unpack(action_digest)

    action = remote_execution_pb2.Action()

    action = parse_to_pb2_from_fetch(action, stub_bytestream, action_digest, parent)

    with tempfile.TemporaryDirectory() as temp_dir:

        command = remote_execution_pb2.Command()
        command = parse_to_pb2_from_fetch(command, stub_bytestream, action.command_digest, parent)

        arguments = "cd {} &&".format(temp_dir)

        for argument in command.arguments:
            arguments += " {}".format(argument)

        context.logger.info(arguments)

        write_fetch_directory(temp_dir, stub_bytestream, action.input_root_digest, parent)

        proc = subprocess.Popen(arguments,
                                shell=True,
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)

        # TODO: Should return the std_out to the user
        proc.communicate()

        result = remote_execution_pb2.ActionResult()
        requests = []
        for output_file in command.output_files:
            path = os.path.join(temp_dir, output_file)
            chunk = read_file(path)

            digest = create_digest(chunk)

            result.output_files.extend([remote_execution_pb2.OutputFile(path=output_file,
                                                                        digest=digest)])

            requests.append(remote_execution_pb2.BatchUpdateBlobsRequest.Request(
                digest=digest, data=chunk))

        request = remote_execution_pb2.BatchUpdateBlobsRequest(instance_name=parent,
                                                               requests=requests)

        stub_cas = remote_execution_pb2_grpc.ContentAddressableStorageStub(context.cas_channel)
        stub_cas.BatchUpdateBlobs(request)

        result_any = any_pb2.Any()
        result_any.Pack(result)

        lease.result.CopyFrom(result_any)

    return lease
