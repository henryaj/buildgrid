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
Operations command
=================

Check the status of operations
"""

import logging
from urllib.parse import urlparse
import sys

import click
import grpc

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.longrunning import operations_pb2, operations_pb2_grpc

from ..cli import pass_context


@click.group(name='operation', short_help="Long running operations commands.")
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


@cli.command('status', short_help="Get the status of an operation.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@pass_context
def status(context, operation_name):
    context.logger.info("Getting operation status...")
    stub = operations_pb2_grpc.OperationsStub(context.channel)

    request = operations_pb2.GetOperationRequest(name=operation_name)

    response = stub.GetOperation(request)
    context.logger.info(response)


@cli.command('list', short_help="List operations.")
@pass_context
def lists(context):
    context.logger.info("Getting list of operations")
    stub = operations_pb2_grpc.OperationsStub(context.channel)

    request = operations_pb2.ListOperationsRequest(name=context.instance_name)

    response = stub.ListOperations(request)

    if not response.operations:
        context.logger.warning("No operations to list")
        return

    for op in response.operations:
        context.logger.info(op)


@cli.command('wait', short_help="Streams an operation until it is complete.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@pass_context
def wait(context, operation_name):
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)
    request = remote_execution_pb2.WaitExecutionRequest(instance_name=context.instance_name,
                                                        name=operation_name)

    response = stub.WaitExecution(request)

    for stream in response:
        context.logger.info(stream)
