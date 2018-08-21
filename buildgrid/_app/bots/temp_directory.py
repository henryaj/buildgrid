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

from buildgrid.client.cas import upload
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.bytestream import bytestream_pb2_grpc
from buildgrid.utils import write_fetch_directory, parse_to_pb2_from_fetch
from buildgrid.utils import output_file_maker, output_directory_maker


def work_temp_directory(context, lease):
    """Executes a lease for a build action, using host tools.
    """

    stub_bytestream = bytestream_pb2_grpc.ByteStreamStub(context.cas_channel)
    instance_name = context.parent
    logger = context.logger

    action_digest = remote_execution_pb2.Digest()
    lease.payload.Unpack(action_digest)

    action = parse_to_pb2_from_fetch(remote_execution_pb2.Action(),
                                     stub_bytestream, action_digest, instance_name)

    with tempfile.TemporaryDirectory() as temp_directory:
        command = parse_to_pb2_from_fetch(remote_execution_pb2.Command(),
                                          stub_bytestream, action.command_digest, instance_name)

        write_fetch_directory(temp_directory, stub_bytestream,
                              action.input_root_digest, instance_name)

        environment = os.environ.copy()
        for variable in command.environment_variables:
            if variable.name not in ['PATH', 'PWD']:
                environment[variable.name] = variable.value

        command_line = list()
        for argument in command.arguments:
            command_line.append(argument.strip())

        working_directory = None
        if command.working_directory:
            working_directory = os.path.join(temp_directory,
                                             command.working_directory)
            os.makedirs(working_directory, exist_ok=True)
        else:
            working_directory = temp_directory

        # Ensure that output files structure exists:
        for output_path in command.output_files:
            directory_path = os.path.join(working_directory,
                                          os.path.dirname(output_path))
            os.makedirs(directory_path, exist_ok=True)

        logger.debug(' '.join(command_line))

        process = subprocess.Popen(command_line,
                                   cwd=working_directory,
                                   universal_newlines=True,
                                   env=environment,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE)
        # TODO: Should return the stdout and stderr in the ActionResult.
        process.communicate()

        action_result = remote_execution_pb2.ActionResult()

        with upload(context.cas_channel, instance=instance_name) as cas:
            for output_path in command.output_files:
                file_path = os.path.join(working_directory, output_path)
                # Missing outputs should simply be omitted in ActionResult:
                if not os.path.isfile(file_path):
                    continue

                output_file = output_file_maker(file_path, working_directory, cas=cas)
                action_result.output_files.extend([output_file])

            for output_path in command.output_directories:
                directory_path = os.path.join(working_directory, output_path)
                # Missing outputs should simply be omitted in ActionResult:
                if not os.path.isdir(directory_path):
                    continue

                # OutputDirectory.path should be relative to the working direcory:
                output_directory = output_directory_maker(directory_path, working_directory, cas=cas)

                action_result.output_directories.extend([output_directory])

        action_result_any = any_pb2.Any()
        action_result_any.Pack(action_result)

        lease.result.CopyFrom(action_result_any)

    return lease
