# Copyright (C) 2018 Codethink Limited
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
#
# Authors:
#        Finn Ball <finn.ball@codethink.co.uk>

"""
Bot command
=================

Create a bot interface and request work
"""

import asyncio
import click
import grpc
import logging
import os
import random
import subprocess
import tempfile

from pathlib import Path, PurePath

from buildgrid.bot import bot
from buildgrid._exceptions import BotError

from ..cli import pass_context

from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from google.protobuf import any_pb2

@click.group(short_help = 'Create a bot client')
@click.option('--parent', default='bgd_test')
@click.option('--number-of-leases', default=1)
@click.option('--port', default='50051')
@click.option('--host', default='localhost')
@pass_context
def cli(context, host, port, number_of_leases, parent):
    context.logger = logging.getLogger(__name__)
    context.logger.info("Starting on port {}".format(port))

    context.channel = grpc.insecure_channel('{}:{}'.format(host, port))
    context.number_of_leases = number_of_leases
    context.parent = parent

@cli.command('dummy', short_help='Create a dummy bot session')
@pass_context
def dummy(context):
    """
    Simple dummy client. Creates a session, accepts leases, does fake work and
    updates the server.
    """

    context.logger.info("Creating a bot session")

    try:
        bot.Bot(work=_work_dummy,
                context=context,
                channel=context.channel,
                parent=context.parent,
                number_of_leases=context.number_of_leases)

    except KeyboardInterrupt:
        pass

@cli.command('buildbox', short_help='Create a bot session with busybox')
@click.option('--fuse-dir', show_default = True, default=str(PurePath(Path.home(), 'fuse')))
@click.option('--local-cas', show_default = True, default=str(PurePath(Path.home(), 'cas')))
@click.option('--client-cert', show_default = True, default=str(PurePath(Path.home(), 'client.crt')))
@click.option('--client-key', show_default = True, default=str(PurePath(Path.home(), 'client.key')))
@click.option('--server-cert', show_default = True, default=str(PurePath(Path.home(), 'server.crt')))
@click.option('--port', show_default = True, default=11001)
@click.option('--remote', show_default = True, default='localhost')
@pass_context
def _work_buildbox(context, remote, port, server_cert, client_key, client_cert, local_cas, fuse_dir):
    """
    Uses BuildBox to run commands.
    """

    context.logger.info("Creating a bot session")

    context.remote = remote
    context.port = port
    context.server_cert = server_cert
    context.client_key = client_key
    context.client_cert = client_cert
    context.local_cas = local_cas
    context.fuse_dir = fuse_dir

    try:
        bot.Bot(work=_work_buildbox,
                context=context,
                channel=context.channel,
                parent=context.parent,
                number_of_leases=context.number_of_leases)

    except KeyboardInterrupt:
        pass

async def _work_dummy(context, lease):
    await asyncio.sleep(random.randint(1,5))
    return lease

async def _work_buildbox(context, lease):
    logger = context.logger

    action_any = lease.payload
    action = remote_execution_pb2.Action()
    action_any.Unpack(action)

    cert_server = _file_read(context.server_cert)
    cert_client = _file_read(context.client_cert)
    key_client = _file_read(context.client_key)

    # create server credentials
    credentials = grpc.ssl_channel_credentials(root_certificates=cert_server,
                                               private_key=key_client,
                                               certificate_chain=cert_client)

    channel = grpc.secure_channel('{}:{}'.format(context.remote, context.port), credentials)

    stub = bytestream_pb2_grpc.ByteStreamStub(channel)

    remote_command = _fetch_command(context.local_cas, stub, action.command_digest)
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
    std_out, std_error = proc.communicate(std_send)

    output_root_digest = remote_execution_pb2.Digest()
    output_root_digest.ParseFromString(std_out)
    logger.debug("Output root digest: {}".format(output_root_digest))

    output_file = remote_execution_pb2.OutputDirectory(tree_digest = output_root_digest)

    action_result = remote_execution_pb2.ActionResult()
    action_result.output_directories.extend([output_file])

    action_result_any = any_pb2.Any()
    action_result_any.Pack(action_result)

    lease.result.CopyFrom(action_result_any)

    return lease

def _fetch_blob(remote, digest, out):
    resource_name = os.path.join(digest.hash, str(digest.size_bytes))
    request = bytestream_pb2.ReadRequest()
    request.resource_name = resource_name
    request.read_offset = 0
    for response in remote.Read(request):
        out.write(response.data)

    out.flush()
    assert digest.size_bytes == os.fstat(out.fileno()).st_size

def _fetch_command(casdir, remote, digest):
    with tempfile.NamedTemporaryFile(dir=os.path.join(casdir, 'tmp')) as out:
        _fetch_blob(remote, digest, out)
        remote_command = remote_execution_pb2.Command()
        with open(out.name, 'rb') as f:
            remote_command.ParseFromString(f.read())
        return remote_command

def _file_read(file_path):
    with open(file_path, 'rb') as f:
        return f.read()
