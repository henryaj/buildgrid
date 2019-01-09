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


import logging
import os
import subprocess
import tempfile

from buildgrid.client.cas import download, upload
from buildgrid._exceptions import BotError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.settings import HASH_LENGTH, MAX_REQUEST_SIZE
from buildgrid.utils import read_file, write_file


def work_buildbox(lease, context, event):
    """Executes a lease for a build action, using buildbox.
    """
    local_cas_directory = context.local_cas
    # instance_name = context.parent

    logger = logging.getLogger(__name__)

    action_digest = remote_execution_pb2.Digest()

    lease.payload.Unpack(action_digest)
    lease.result.Clear()

    with download(context.cas_channel) as downloader:
        action = downloader.get_message(action_digest,
                                        remote_execution_pb2.Action())

        assert action.command_digest.hash

        command = downloader.get_message(action.command_digest,
                                         remote_execution_pb2.Command())

    if command.working_directory:
        working_directory = command.working_directory
    else:
        working_directory = '/'

    logger.debug("Command digest: [{}/{}]"
                 .format(action.command_digest.hash, action.command_digest.size_bytes))
    logger.debug("Input root digest: [{}/{}]"
                 .format(action.input_root_digest.hash, action.input_root_digest.size_bytes))

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

            command_line.append('--clearenv')
            for variable in command.environment_variables:
                command_line.append('--setenv')
                command_line.append(variable.name)
                command_line.append(variable.value)

            command_line.append(context.fuse_dir)
            command_line.extend(command.arguments)

            logger.info("Starting execution: [{}...]".format(command.arguments[0]))

            command_line = subprocess.Popen(command_line,
                                            stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
            stdout, stderr = command_line.communicate()
            returncode = command_line.returncode

            action_result = remote_execution_pb2.ActionResult()
            action_result.exit_code = returncode

            logger.info("Execution finished with code: [{}]".format(returncode))

            output_digest = remote_execution_pb2.Digest()
            output_digest.ParseFromString(read_file(output_digest_file.name))

            logger.debug("Output root digest: [{}/{}]"
                         .format(output_digest.hash, output_digest.size_bytes))

            if len(output_digest.hash) != HASH_LENGTH:
                raise BotError(stdout,
                               detail=stderr, reason="Output root digest too small.")

            # TODO: Have BuildBox helping us creating the Tree instance here
            # See https://gitlab.com/BuildStream/buildbox/issues/7 for details
            with download(context.cas_channel) as downloader:
                output_tree = _cas_tree_maker(downloader, output_digest)

            with upload(context.cas_channel) as uploader:
                output_tree_digest = uploader.put_message(output_tree)

                output_directory = remote_execution_pb2.OutputDirectory()
                output_directory.tree_digest.CopyFrom(output_tree_digest)
                output_directory.path = os.path.relpath(working_directory, start='/')

                action_result.output_directories.extend([output_directory])

                if action_result.ByteSize() + len(stdout) > MAX_REQUEST_SIZE:
                    stdout_digest = uploader.put_blob(stdout)
                    action_result.stdout_digest.CopyFrom(stdout_digest)

                else:
                    action_result.stdout_raw = stdout

                if action_result.ByteSize() + len(stderr) > MAX_REQUEST_SIZE:
                    stderr_digest = uploader.put_blob(stderr)
                    action_result.stderr_digest.CopyFrom(stderr_digest)

                else:
                    action_result.stderr_raw = stderr

            lease.result.Pack(action_result)

    return lease


def _cas_tree_maker(cas, directory_digest):
    # Generates and stores a Tree for a given Directory. This is very inefficient
    # and only temporary. See https://gitlab.com/BuildStream/buildbox/issues/7.
    output_tree = remote_execution_pb2.Tree()

    def __cas_tree_maker(cas, parent_directory):
        digests, directories = [], []
        for directory_node in parent_directory.directories:
            directories.append(remote_execution_pb2.Directory())
            digests.append(directory_node.digest)

        cas.get_messages(digests, directories)

        for directory in directories[:]:
            directories.extend(__cas_tree_maker(cas, directory))

        return directories

    root_directory = cas.get_message(directory_digest,
                                     remote_execution_pb2.Directory())

    output_tree.children.extend(__cas_tree_maker(cas, root_directory))
    output_tree.root.CopyFrom(root_directory)

    return output_tree
