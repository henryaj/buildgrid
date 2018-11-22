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
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.utils import get_hostname, output_file_maker, output_directory_maker


def work_host_tools(context, lease):
    """Executes a lease for a build action, using host tools.
    """
    instance_name = context.parent
    logger = logging.getLogger(__name__)

    action_digest = remote_execution_pb2.Digest()
    action_result = remote_execution_pb2.ActionResult()

    lease.payload.Unpack(action_digest)
    lease.result.Clear()

    action_result.execution_metadata.worker = get_hostname()

    with tempfile.TemporaryDirectory() as temp_directory:
        with download(context.cas_channel, instance=instance_name) as downloader:
            action = downloader.get_message(action_digest,
                                            remote_execution_pb2.Action())

            assert action.command_digest.hash

            command = downloader.get_message(action.command_digest,
                                             remote_execution_pb2.Command())

            action_result.execution_metadata.input_fetch_start_timestamp.GetCurrentTime()

            downloader.download_directory(action.input_root_digest, temp_directory)

        action_result.execution_metadata.input_fetch_completed_timestamp.GetCurrentTime()

        environment = os.environ.copy()
        for variable in command.environment_variables:
            if variable.name not in ['PATH', 'PWD']:
                environment[variable.name] = variable.value

        command_line = []
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

        action_result.execution_metadata.execution_start_timestamp.GetCurrentTime()

        process = subprocess.Popen(command_line,
                                   cwd=working_directory,
                                   env=environment,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

        stdout, stderr = process.communicate()
        returncode = process.returncode

        action_result.execution_metadata.execution_completed_timestamp.GetCurrentTime()

        # TODO: Upload to CAS or output RAW
        # For now, just pass raw
        # https://gitlab.com/BuildGrid/buildgrid/issues/90
        action_result.stdout_raw = stdout
        action_result.stderr_raw = stderr
        action_result.exit_code = returncode

        logger.debug("Command stderr: [{}]".format(stderr))
        logger.debug("Command stdout: [{}]".format(stdout))
        logger.debug("Command exit code: [{}]".format(returncode))

        action_result.execution_metadata.output_upload_start_timestamp.GetCurrentTime()

        with upload(context.cas_channel, instance=instance_name) as uploader:
            output_files, output_directories = [], []

            for output_path in command.output_files:
                file_path = os.path.join(working_directory, output_path)
                # Missing outputs should simply be omitted in ActionResult:
                if not os.path.isfile(file_path):
                    continue

                file_digest = uploader.upload_file(file_path, queue=True)
                output_file = output_file_maker(file_path, working_directory,
                                                file_digest)
                output_files.append(output_file)

            action_result.output_files.extend(output_files)

            for output_path in command.output_directories:
                directory_path = os.path.join(working_directory, output_path)
                # Missing outputs should simply be omitted in ActionResult:
                if not os.path.isdir(directory_path):
                    continue

                tree_digest = uploader.upload_tree(directory_path, queue=True)
                output_directory = output_directory_maker(directory_path, working_directory,
                                                          tree_digest)
                output_directories.append(output_directory)

            action_result.output_directories.extend(output_directories)

        action_result.execution_metadata.output_upload_completed_timestamp.GetCurrentTime()

        lease.result.Pack(action_result)

    return lease
