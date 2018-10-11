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


"""
Execute command
=================

Request work to be executed and monitor status of jobs.
"""

import logging
import os
import stat
import sys
from urllib.parse import urlparse

import click
import grpc

from buildgrid.client.cas import download, upload
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid.utils import create_digest

from ..cli import pass_context


@click.group(name='execute', short_help="Execute simple operations.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote execution server's URL (port defaults to 50051 if no specified).")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded)")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded)")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded)")
@click.option('--instance-name', type=click.STRING, default='main', show_default=True,
              help="Targeted farm instance name.")
@pass_context
def cli(context, remote, instance_name, client_key, client_cert, server_cert):
    url = urlparse(remote)

    context.remote = '{}:{}'.format(url.hostname, url.port or 50051)
    context.instance_name = instance_name

    if url.scheme == 'http':
        context.channel = grpc.insecure_channel(context.remote)
    else:
        credentials = context.load_client_credentials(client_key, client_cert, server_cert)
        if not credentials:
            click.echo("ERROR: no TLS keys were specified and no defaults could be found.", err=True)
            sys.exit(-1)

        context.channel = grpc.secure_channel(context.remote, credentials)

    context.logger = logging.getLogger(__name__)
    context.logger.debug("Starting for remote {}".format(context.remote))


@cli.command('request-dummy', short_help="Send a dummy action.")
@click.option('--number', type=click.INT, default=1, show_default=True,
              help="Number of request to send.")
@click.option('--wait-for-completion', is_flag=True,
              help="Stream updates until jobs are completed.")
@pass_context
def request_dummy(context, number, wait_for_completion):

    context.logger.info("Sending execution request...")
    action = remote_execution_pb2.Action(do_not_cache=True)
    action_digest = create_digest(action.SerializeToString())

    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)

    request = remote_execution_pb2.ExecuteRequest(instance_name=context.instance_name,
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=True)

    responses = []
    for _ in range(0, number):
        responses.append(stub.Execute(request))

    for response in responses:

        if wait_for_completion:
            result = None
            for stream in response:
                result = stream
                context.logger.info(result)

            if not result.done:
                click.echo("Result did not return True." +
                           "Was the action uploaded to CAS?", err=True)
                sys.exit(-1)

        else:
            context.logger.info(next(response))


@cli.command('command', short_help="Send a command to be executed.")
@click.option('--output-file', nargs=2, type=(click.STRING, click.BOOL), multiple=True,
              help="Tuple of expected output file and is-executeable flag.")
@click.option('--output-directory', default='testing', show_default=True,
              help="Output directory for the output files.")
@click.argument('input-root', nargs=1, type=click.Path(), required=True)
@click.argument('commands', nargs=-1, type=click.STRING, required=True)
@pass_context
def run_command(context, input_root, commands, output_file, output_directory):
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)

    output_executeables = []
    with upload(context.channel, instance=context.instance_name) as uploader:
        command = remote_execution_pb2.Command()

        for arg in commands:
            command.arguments.extend([arg])

        for file, is_executeable in output_file:
            command.output_files.extend([file])
            if is_executeable:
                output_executeables.append(file)

        command_digest = uploader.put_message(command, queue=True)

        context.logger.info('Sent command: {}'.format(command_digest))

        # TODO: Check for missing blobs
        input_root_digest = uploader.upload_directory(input_root)

        context.logger.info('Sent input: {}'.format(input_root_digest))

        action = remote_execution_pb2.Action(command_digest=command_digest,
                                             input_root_digest=input_root_digest,
                                             do_not_cache=True)

        action_digest = uploader.put_message(action, queue=True)

        context.logger.info("Sent action: {}".format(action_digest))

    request = remote_execution_pb2.ExecuteRequest(instance_name=context.instance_name,
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=True)
    response = stub.Execute(request)

    stream = None
    for stream in response:
        context.logger.info(stream)

    execute_response = remote_execution_pb2.ExecuteResponse()
    stream.response.Unpack(execute_response)

    with download(context.channel, instance=context.instance_name) as downloader:

        for output_file_response in execute_response.result.output_files:
            path = os.path.join(output_directory, output_file_response.path)

            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path), exist_ok=True)

            downloader.download_file(output_file_response.digest, path)

    for output_file_response in execute_response.result.output_files:
        if output_file_response.path in output_executeables:
            st = os.stat(path)
            os.chmod(path, st.st_mode | stat.S_IXUSR)
