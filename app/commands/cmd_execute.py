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
Execute command
=================

Request work to be executed and monitor status of jobs.
"""

import click
import grpc
import logging
import sys
import time

from ..cli import pass_context

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import ExecuteOperationMetadata
from buildgrid._protos.google.longrunning import operations_pb2, operations_pb2_grpc
from google.protobuf import any_pb2

@click.group(short_help = "Simple execute client")
@click.option('--port', default='50051')
@click.option('--host', default='localhost')
@pass_context
def cli(context, host, port):
    context.logger = logging.getLogger(__name__)
    context.logger.info("Starting on port {}".format(port))

    context.channel = grpc.insecure_channel('{}:{}'.format(host, port))
    context.port = port

@cli.command('request', short_help='Send a dummy action')
@click.option('--number', default=1)
@click.option('--instance-name', default='testing')
@click.option('--wait-for-completion', is_flag=True)
@pass_context
def request(context, number, instance_name, wait_for_completion):
    action_digest = remote_execution_pb2.Digest()

    context.logger.info("Sending execution request...\n")
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)

    request = remote_execution_pb2.ExecuteRequest(instance_name = instance_name,
                                                  action_digest = action_digest,
                                                  skip_cache_lookup = True)
    responses = []
    for i in range(0, number):
        responses.append(stub.Execute(request))

    for response in responses:
        if wait_for_completion:
            for stream in response:
                context.logger.info(stream)
        else:
            context.logger.info(next(response))

@cli.command('status', short_help='Get the status of an operation')
@click.argument('operation-name')
@pass_context
def operation_status(context, operation_name):
    context.logger.info("Getting operation status...\n")
    stub = operations_pb2_grpc.OperationsStub(context.channel)

    request = operations_pb2.GetOperationRequest(name=operation_name)

    response = stub.GetOperation(request)
    context.logger.info(response)

@cli.command('list', short_help='List operations')
@pass_context
def list_operations(context):
    context.logger.info("Getting list of operations")
    stub = operations_pb2_grpc.OperationsStub(context.channel)

    request = operations_pb2.ListOperationsRequest()

    response = stub.ListOperations(request)

    if len(response.operations) < 1:
        context.logger.warning("No operations to list")
        return

    for op in response.operations:
        context.logger.info(op)

@cli.command('wait', short_help='Streams an operation until it is complete')
@click.argument('operation-name')
@pass_context
def wait_execution(context, operation_name):
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)
    request = remote_execution_pb2.WaitExecutionRequest(name=operation_name)

    response = stub.WaitExecution(request)

    for stream in response:
        context.logger.info(stream)