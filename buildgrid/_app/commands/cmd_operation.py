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

from collections import OrderedDict
from operator import attrgetter
import sys
from textwrap import indent

import click
from google.protobuf import json_format

from buildgrid.client.authentication import setup_channel
from buildgrid._enums import OperationStage
from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.longrunning import operations_pb2, operations_pb2_grpc
from buildgrid._protos.google.rpc import code_pb2

from ..cli import pass_context


@click.group(name='operation', short_help="Long running operations commands.")
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
@click.option('--instance-name', type=click.STRING, default='main', show_default=True,
              help="Targeted farm instance name.")
@pass_context
def cli(context, remote, instance_name, auth_token, client_key, client_cert, server_cert):
    """Entry point for the bgd-operation CLI command group."""
    try:
        context.channel, _ = setup_channel(remote, auth_token=auth_token,
                                           client_key=client_key, client_cert=client_cert)

    except InvalidArgumentError as e:
        click.echo("Error: {}.".format(e), err=True)
        sys.exit(-1)

    context.instance_name = instance_name


def _print_operation_status(operation, print_details=False):
    metadata = remote_execution_pb2.ExecuteOperationMetadata()
    # The metadata is expected to be an ExecuteOperationMetadata message:
    assert operation.metadata.Is(metadata.DESCRIPTOR)
    operation.metadata.Unpack(metadata)

    stage = OperationStage(metadata.stage)

    if not operation.done:
        if stage == OperationStage.CACHE_CHECK:
            click.echo('CacheCheck: {}: Querying action-cache (stage={})'
                       .format(operation.name, metadata.stage))
        elif stage == OperationStage.QUEUED:
            click.echo('Queued: {}: Waiting for execution (stage={})'
                       .format(operation.name, metadata.stage))
        elif stage == OperationStage.EXECUTING:
            click.echo('Executing: {}: Currently running (stage={})'
                       .format(operation.name, metadata.stage))
        else:
            click.echo('Error: {}: In an invalid state (stage={})'
                       .format(operation.name, metadata.stage), err=True)
        return

    assert stage == OperationStage.COMPLETED

    response = remote_execution_pb2.ExecuteResponse()
    # The response is expected to be an ExecutionResponse message:
    assert operation.response.Is(response.DESCRIPTOR)
    operation.response.Unpack(response)

    if response.status.code != code_pb2.OK:
        click.echo('Failure: {}: {} (code={})'
                   .format(operation.name, response.status.message, response.status.code))
    else:
        if response.result.exit_code != 0:
            click.echo('Success: {}: Completed with failure (stage={}, exit_code={})'
                       .format(operation.name, metadata.stage, response.result.exit_code))
        else:
            click.echo('Success: {}: Completed succesfully (stage={}, exit_code={})'
                       .format(operation.name, metadata.stage, response.result.exit_code))

    if print_details:
        metadata = response.result.execution_metadata
        click.echo(indent('worker={}'.format(metadata.worker), '  '))

        queued = metadata.queued_timestamp.ToDatetime()
        click.echo(indent('queued_at={}'.format(queued), '  '))

        worker_start = metadata.worker_start_timestamp.ToDatetime()
        worker_completed = metadata.worker_completed_timestamp.ToDatetime()
        click.echo(indent('work_duration={}'.format(worker_completed - worker_start), '  '))

        fetch_start = metadata.input_fetch_start_timestamp.ToDatetime()
        fetch_completed = metadata.input_fetch_completed_timestamp.ToDatetime()
        click.echo(indent('fetch_duration={}'.format(fetch_completed - fetch_start), '    '))

        execution_start = metadata.execution_start_timestamp.ToDatetime()
        execution_completed = metadata.execution_completed_timestamp.ToDatetime()
        click.echo(indent('exection_duration={}'.format(execution_completed - execution_start), '    '))

        upload_start = metadata.output_upload_start_timestamp.ToDatetime()
        upload_completed = metadata.output_upload_completed_timestamp.ToDatetime()
        click.echo(indent('upload_duration={}'.format(upload_completed - upload_start), '    '))

        click.echo(indent('total_duration={}'.format(worker_completed - queued), '  '))


@cli.command('status', short_help="Get the status of an operation.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@click.option('--json', is_flag=True, show_default=True,
              help="Print operations status in JSON format.")
@pass_context
def status(context, operation_name, json):
    stub = operations_pb2_grpc.OperationsStub(context.channel)
    request = operations_pb2.GetOperationRequest(name=operation_name)

    operation = stub.GetOperation(request)

    if not json:
        _print_operation_status(operation, print_details=True)
    else:
        click.echo(json_format.MessageToJson(operation))


@cli.command('cancel', short_help="Cancel an operation.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@pass_context
def cancel(context, operation_name):
    click.echo("Cancelling an operation...")
    stub = operations_pb2_grpc.OperationsStub(context.channel)
    request = operations_pb2.CancelOperationRequest(name=operation_name)

    stub.CancelOperation(request)
    click.echo("Operation cancelled: [{}]".format(request))


@cli.command('list', short_help="List operations.")
@click.option('--json', is_flag=True, show_default=True,
              help="Print operations list in JSON format.")
@pass_context
def lists(context, json):
    stub = operations_pb2_grpc.OperationsStub(context.channel)
    request = operations_pb2.ListOperationsRequest(name=context.instance_name)

    response = stub.ListOperations(request)

    if not response.operations:
        click.echo('Error: No operations to list.', err=True)
        return

    operations_map = OrderedDict([
        (OperationStage.CACHE_CHECK, []),
        (OperationStage.QUEUED, []),
        (OperationStage.EXECUTING, []),
        (OperationStage.COMPLETED, [])
    ])

    for operation in response.operations:
        metadata = remote_execution_pb2.ExecuteOperationMetadata()
        # The metadata is expected to be an ExecuteOperationMetadata message:
        assert operation.metadata.Is(metadata.DESCRIPTOR)
        operation.metadata.Unpack(metadata)

        stage = OperationStage(metadata.stage)

        operations_map[stage].append(operation)

    for operations in operations_map.values():
        operations.sort(key=attrgetter('name'))
        for operation in operations:
            if not json:
                _print_operation_status(operation)
            else:
                click.echo(json_format.MessageToJson(operation))


@cli.command('wait', short_help="Streams an operation until it is complete.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@click.option('--json', is_flag=True, show_default=True,
              help="Print operations statuses in JSON format.")
@pass_context
def wait(context, operation_name, json):
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)
    request = remote_execution_pb2.WaitExecutionRequest(name=operation_name)

    operation_iterator = stub.WaitExecution(request)

    for operation in operation_iterator:
        if not json and operation.done:
            _print_operation_status(operation, print_details=True)
        elif not json:
            _print_operation_status(operation)
        else:
            click.echo(json_format.MessageToJson(operation))
