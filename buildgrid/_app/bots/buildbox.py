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
import grpc

from google.protobuf import any_pb2

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid.utils import read_file


def work_buildbox(context, lease):
    logger = context.logger

    digest_any = lease.payload
    digest = remote_execution_pb2.Digest()
    digest_any.Unpack(digest)

    cert_server = read_file(context.server_cert)
    cert_client = read_file(context.client_cert)
    key_client = read_file(context.client_key)

    # create server credentials
    credentials = grpc.ssl_channel_credentials(root_certificates=cert_server,
                                               private_key=key_client,
                                               certificate_chain=cert_client)

    channel = grpc.secure_channel('{}:{}'.format(context.remote, context.port), credentials)

    stub = bytestream_pb2_grpc.ByteStreamStub(channel)

    casdir = context.local_cas
    action = _buildstream_fetch_action(casdir, stub, digest)

    remote_command = _buildstream_fetch_command(context.local_cas, stub, action.command_digest)
    environment = dict((x.name, x.value) for x in remote_command.environment_variables)
    logger.debug("command hash: {}".format(action.command_digest.hash))
    logger.debug("vdir hash: {}".format(action.input_root_digest.hash))
    logger.debug("\n{}".format(' '.join(remote_command.arguments)))

    command = ['buildbox',
               '--remote={}'.format('https://{}:{}'.format(context.remote, context.port)),
               '--server-cert={}'.format(context.server_cert),
               '--client-key={}'.format(context.client_key),
               '--client-cert={}'.format(context.client_cert),
               '--local={}'.format(context.local_cas),
               '--chdir={}'.format(environment['PWD']),
               context.fuse_dir]

    command.extend(remote_command.arguments)

    logger.debug(' '.join(command))
    logger.debug("Input root digest:\n{}".format(action.input_root_digest))
    logger.info("Launching process")

    proc = subprocess.Popen(command,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE)
    std_send = action.input_root_digest.SerializeToString()
    std_out, _ = proc.communicate(std_send)

    output_root_digest = remote_execution_pb2.Digest()
    output_root_digest.ParseFromString(std_out)
    logger.debug("Output root digest: {}".format(output_root_digest))

    output_file = remote_execution_pb2.OutputDirectory(tree_digest=output_root_digest)

    action_result = remote_execution_pb2.ActionResult()
    action_result.output_directories.extend([output_file])

    action_result_any = any_pb2.Any()
    action_result_any.Pack(action_result)

    lease.result.CopyFrom(action_result_any)

    return lease


def _buildstream_fetch_blob(remote, digest, out):
    resource_name = os.path.join(digest.hash, str(digest.size_bytes))
    request = bytestream_pb2.ReadRequest()
    request.resource_name = resource_name
    request.read_offset = 0
    for response in remote.Read(request):
        out.write(response.data)

    out.flush()
    assert digest.size_bytes == os.fstat(out.fileno()).st_size


def _buildstream_fetch_command(casdir, remote, digest):
    with tempfile.NamedTemporaryFile(dir=os.path.join(casdir, 'tmp')) as out:
        _buildstream_fetch_blob(remote, digest, out)
        remote_command = remote_execution_pb2.Command()
        with open(out.name, 'rb') as f:
            remote_command.ParseFromString(f.read())
        return remote_command


def _buildstream_fetch_action(casdir, remote, digest):
    with tempfile.NamedTemporaryFile(dir=os.path.join(casdir, 'tmp')) as out:
        _buildstream_fetch_blob(remote, digest, out)
        remote_action = remote_execution_pb2.Action()
        with open(out.name, 'rb') as f:
            remote_action.ParseFromString(f.read())
        return remote_action
