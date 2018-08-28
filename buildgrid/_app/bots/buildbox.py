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

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.bytestream import bytestream_pb2_grpc
from buildgrid.utils import parse_to_pb2_from_fetch


def work_buildbox(context, lease):
    logger = context.logger

    action_digest_any = lease.payload
    action_digest = remote_execution_pb2.Digest()
    action_digest_any.Unpack(action_digest)

    stub = bytestream_pb2_grpc.ByteStreamStub(context.cas_channel)

    action = remote_execution_pb2.Action()
    parse_to_pb2_from_fetch(action, stub, action_digest)

    casdir = context.local_cas
    remote_command = remote_execution_pb2.Command()
    parse_to_pb2_from_fetch(remote_command, stub, action.command_digest)

    environment = dict((x.name, x.value) for x in remote_command.environment_variables)
    logger.debug("command hash: {}".format(action.command_digest.hash))
    logger.debug("vdir hash: {}".format(action.input_root_digest.hash))
    logger.debug("\n{}".format(' '.join(remote_command.arguments)))

    # Input hash must be written to disk for buildbox.
    os.makedirs(os.path.join(casdir, 'tmp'), exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=os.path.join(casdir, 'tmp')) as input_digest_file:
        with open(input_digest_file.name, 'wb') as f:
            f.write(action.input_root_digest.SerializeToString())
            f.flush()

        with tempfile.NamedTemporaryFile(dir=os.path.join(casdir, 'tmp')) as output_digest_file:
            command = ['buildbox',
                       '--remote={}'.format(context.remote_cas_url),
                       '--input-digest={}'.format(input_digest_file.name),
                       '--output-digest={}'.format(output_digest_file.name),
                       '--local={}'.format(casdir)]

            if context.cas_client_key:
                command.append('--client-key={}'.format(context.cas_client_key))
            if context.cas_client_cert:
                command.append('--client-cert={}'.format(context.cas_client_cert))
            if context.cas_server_cert:
                command.append('--server-cert={}'.format(context.cas_server_cert))

            if 'PWD' in environment and environment['PWD']:
                command.append('--chdir={}'.format(environment['PWD']))

            command.append(context.fuse_dir)
            command.extend(remote_command.arguments)

            logger.debug(' '.join(command))
            logger.debug("Input root digest:\n{}".format(action.input_root_digest))
            logger.info("Launching process")

            proc = subprocess.Popen(command,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE)
            proc.communicate()

            output_root_digest = remote_execution_pb2.Digest()
            with open(output_digest_file.name, 'rb') as f:
                output_root_digest.ParseFromString(f.read())
            logger.debug("Output root digest: {}".format(output_root_digest))

            if len(output_root_digest.hash) < 64:
                logger.warning("Buildbox command failed - no output root digest present.")
            output_file = remote_execution_pb2.OutputDirectory(tree_digest=output_root_digest)

    action_result = remote_execution_pb2.ActionResult()
    action_result.output_directories.extend([output_file])

    action_result_any = any_pb2.Any()
    action_result_any.Pack(action_result)

    lease.result.CopyFrom(action_result_any)

    return lease
