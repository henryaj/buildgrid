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

from buildgrid.settings import HASH_LENGTH
from buildgrid.client.cas import upload
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.bytestream import bytestream_pb2_grpc
from buildgrid._exceptions import BotError
from buildgrid.utils import read_file, write_file, parse_to_pb2_from_fetch


def work_buildbox(context, lease):
    """Executes a lease for a build action, using buildbox.
    """

    stub_bytestream = bytestream_pb2_grpc.ByteStreamStub(context.cas_channel)
    local_cas_directory = context.local_cas
    logger = context.logger

    action_digest = remote_execution_pb2.Digest()
    lease.payload.Unpack(action_digest)

    action = parse_to_pb2_from_fetch(remote_execution_pb2.Action(),
                                     stub_bytestream, action_digest)

    command = parse_to_pb2_from_fetch(remote_execution_pb2.Command(),
                                      stub_bytestream, action.command_digest)

    environment = dict()
    for variable in command.environment_variables:
        if variable.name not in ['PWD']:
            environment[variable.name] = variable.value

    if command.working_directory:
        working_directory = command.working_directory
    else:
        working_directory = '/'

    logger.debug("command hash: {}".format(action.command_digest.hash))
    logger.debug("vdir hash: {}".format(action.input_root_digest.hash))
    logger.debug("\n{}".format(' '.join(command.arguments)))

    os.makedirs(os.path.join(local_cas_directory, 'tmp'), exist_ok=True)
    os.makedirs(context.fuse_dir, exist_ok=True)

    with tempfile.NamedTemporaryFile(dir=os.path.join(local_cas_directory, 'tmp')) as input_digest_file:
        # Input hash must be written to disk for BuildBox
        write_file(input_digest_file.name, action.input_root_digest.SerializeToString())

        with tempfile.NamedTemporaryFile(dir=os.path.join(local_cas_directory, 'tmp')) as output_digest_file:
            command_line = ['buildbox',
                            '--remote={}'.format(context.remote_cas_url),
                            '--input-digest={}'.format(input_digest_file.name),
                            '--output-digest={}'.format(output_digest_file.name),
                            '--chdir={}'.format(working_directory),
                            '--local={}'.format(local_cas_directory)]

            if context.cas_client_key:
                command_line.append('--client-key={}'.format(context.cas_client_key))
            if context.cas_client_cert:
                command_line.append('--client-cert={}'.format(context.cas_client_cert))
            if context.cas_server_cert:
                command_line.append('--server-cert={}'.format(context.cas_server_cert))

            command_line.append(context.fuse_dir)
            command_line.extend(command.arguments)

            logger.debug(' '.join(command_line))
            logger.debug("Input root digest:\n{}".format(action.input_root_digest))
            logger.info("Launching process")

            command_line = subprocess.Popen(command_line,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
            stdout, stderr = command_line.communicate()
            returncode = command_line.returncode
            action_result = remote_execution_pb2.ActionResult()
            # TODO: Upload to CAS or output RAW
            # For now, just pass raw
            # https://gitlab.com/BuildGrid/buildgrid/issues/90
            action_result.stdout_raw = stdout
            action_result.stderr_raw = stderr
            action_result.exit_code = returncode

            logger.debug("BuildBox stderr: [{}]".format(stderr))
            logger.debug("BuildBox stdout: [{}]".format(stdout))
            logger.debug("BuildBox exit code: [{}]".format(returncode))

            output_digest = remote_execution_pb2.Digest()
            output_digest.ParseFromString(read_file(output_digest_file.name))

            logger.debug("Output root digest: [{}]".format(output_digest))

            if len(output_digest.hash) != HASH_LENGTH:
                raise BotError(stdout,
                               detail=stderr, reason="Output root digest too small.")

            # TODO: Have BuildBox helping us creating the Tree instance here
            # See https://gitlab.com/BuildStream/buildbox/issues/7 for details
            output_tree = _cas_tree_maker(stub_bytestream, output_digest)

            with upload(context.cas_channel) as cas:
                output_tree_digest = cas.send_message(output_tree)

            output_directory = remote_execution_pb2.OutputDirectory()
            output_directory.tree_digest.CopyFrom(output_tree_digest)
            output_directory.path = os.path.relpath(working_directory, start='/')

            action_result.output_directories.extend([output_directory])

            action_result_any = any_pb2.Any()
            action_result_any.Pack(action_result)

            lease.result.CopyFrom(action_result_any)

    return lease


def _cas_tree_maker(stub_bytestream, directory_digest):
    # Generates and stores a Tree for a given Directory. This is very inefficient
    # and only temporary. See https://gitlab.com/BuildStream/buildbox/issues/7.
    output_tree = remote_execution_pb2.Tree()

    def list_directories(parent_directory):
        directory_list = list()
        for directory_node in parent_directory.directories:
            directory = parse_to_pb2_from_fetch(remote_execution_pb2.Directory(),
                                                stub_bytestream, directory_node.digest)
            directory_list.extend(list_directories(directory))
            directory_list.append(directory)

        return directory_list

    root_directory = parse_to_pb2_from_fetch(remote_execution_pb2.Directory(),
                                             stub_bytestream, directory_digest)
    output_tree.children.extend(list_directories(root_directory))
    output_tree.root.CopyFrom(root_directory)

    return output_tree
