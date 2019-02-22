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

import os
import stat
import sys

import click

from buildgrid.client.authentication import setup_channel
from buildgrid.client.cas import download, upload
from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid.utils import create_digest

from ..cli import pass_context


@click.group(name='execute', short_help="Execute simple operations.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote execution server's URL (port defaults to 50051 if no specified).")
@click.option('--auth-token', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Authorization token for the remote.")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded).")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded).")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded).")
@click.option('--instance-name', type=click.STRING, default=None, show_default=True,
              help="Targeted farm instance name.")
@click.option('--remote-cas', type=click.STRING, default=None, show_default=False,
              help="Remote CAS server's URL (defaults to --remote if not specified).")
@click.option('--cas-client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private CAS client key for TLS (PEM-encoded).")
@click.option('--cas-client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public CAS client certificate for TLS (PEM-encoded).")
@click.option('--cas-server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public CAS server certificate for TLS (PEM-encoded).")
@pass_context
def cli(context, remote, instance_name, auth_token, client_key, client_cert, server_cert,
        remote_cas, cas_client_key, cas_client_cert, cas_server_cert):
    """Entry point for the bgd-execute CLI command group."""
    try:
        context.channel, details = setup_channel(remote, auth_token=auth_token,
                                                 client_key=client_key,
                                                 client_cert=client_cert,
                                                 server_cert=server_cert)

        if remote_cas and remote_cas != remote:
            context.cas_channel, details = setup_channel(remote_cas,
                                                         server_cert=cas_server_cert,
                                                         client_key=cas_client_key,
                                                         client_cert=cas_client_cert)
            context.remote_cas_url = remote_cas

        else:
            context.cas_channel = context.channel
            context.remote_cas_url = remote

        context.cas_client_key, context.cas_client_cert, context.cas_server_cert = details

    except InvalidArgumentError as e:
        click.echo("Error: {}.".format(e), err=True)
        sys.exit(-1)

    context.instance_name = instance_name


@cli.command('request-dummy', short_help="Send a dummy action.")
@click.option('--number', type=click.INT, default=1, show_default=True,
              help="Number of request to send.")
@click.option('--wait-for-completion', is_flag=True,
              help="Stream updates until jobs are completed.")
@pass_context
def request_dummy(context, number, wait_for_completion):

    click.echo("Sending execution request...")
    command = remote_execution_pb2.Command()
    command_digest = create_digest(command.SerializeToString())

    action = remote_execution_pb2.Action(command_digest=command_digest,
                                         do_not_cache=True)
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
                click.echo(result)

            if not result.done:
                click.echo("Result did not return True." +
                           "Was the action uploaded to CAS?", err=True)
                sys.exit(-1)

        else:
            click.echo(next(response))


@cli.command('command', short_help="Send a command to be executed.")
@click.option('--output-file', nargs=2, type=(click.STRING, click.BOOL), multiple=True,
              help="Tuple of expected output file and is-executeable flag.")
@click.option('--output-directory', default='testing', show_default=True,
              help="Output directory for the output files.")
@click.option('-p', '--platform-property', nargs=2, type=(click.STRING, click.STRING), multiple=True,
              help="List of key-value pairs of required platform properties.")
@click.argument('input-root', nargs=1, type=click.Path(), required=True)
@click.argument('commands', nargs=-1, type=click.STRING, required=True)
@pass_context
def run_command(context, input_root, commands, output_file, output_directory,
                platform_property):
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)

    output_executables = []

    with upload(context.cas_channel, instance=context.instance_name) as uploader:
        command = remote_execution_pb2.Command()

        for arg in commands:
            command.arguments.extend([arg])

        for file, is_executable in output_file:
            command.output_files.extend([file])
            if is_executable:
                output_executables.append(file)

        for attribute_name, attribute_value in platform_property:
            new_property = command.platform.properties.add()
            new_property.name = attribute_name
            new_property.value = attribute_value

        command_digest = uploader.put_message(command, queue=True)

        click.echo("Sent command=[{}]".format(command_digest))

        # TODO: Check for missing blobs
        input_root_digest = uploader.upload_directory(input_root)

        click.echo("Sent input=[{}]".format(input_root_digest))

        action = remote_execution_pb2.Action(command_digest=command_digest,
                                             input_root_digest=input_root_digest,
                                             do_not_cache=True)

        action_digest = uploader.put_message(action, queue=True)

        click.echo("Sent action=[{}]".format(action_digest))

    request = remote_execution_pb2.ExecuteRequest(instance_name=context.instance_name,
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=True)
    response = stub.Execute(request)

    stream = None
    for stream in response:
        click.echo(stream)

    execute_response = remote_execution_pb2.ExecuteResponse()
    stream.response.Unpack(execute_response)

    with download(context.cas_channel, instance=context.instance_name) as downloader:

        for output_file_response in execute_response.result.output_files:
            path = os.path.join(output_directory, output_file_response.path)

            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path), exist_ok=True)

            downloader.download_file(output_file_response.digest, path)

    for output_file_response in execute_response.result.output_files:
        if output_file_response.path in output_executables:
            st = os.stat(path)
            os.chmod(path, st.st_mode | stat.S_IXUSR)
