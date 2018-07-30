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

from buildgrid._protos.google.devtools.remoteexecution.v1test import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.devtools.remoteexecution.v1test.remote_execution_pb2 import ExecuteOperationMetadata
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
    context.logger.info("Sending execution request...\n")
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)

    action = remote_execution_pb2.Action(command_digest = None,
                                         input_root_digest = None,
                                         output_files = [],
                                         output_directories = None,
                                         platform = None,
                                         timeout = None,
                                         do_not_cache = True)

    action.command_digest.hash = 'foo'

    request = remote_execution_pb2.ExecuteRequest(instance_name = instance_name,
                                                  action = action,
                                                  skip_cache_lookup = True)
    for i in range(0, number):
        response = stub.Execute(request)
        context.logger.info("Response name: {}".format(response.name))

    try:
        while wait_for_completion:
            request = operations_pb2.ListOperationsRequest()
            context.logger.debug('Querying to see if jobs are complete.')
            stub = operations_pb2_grpc.OperationsStub(context.channel)
            response = stub.ListOperations(request)
            if all(operation.done for operation in response.operations):
                context.logger.info('Jobs complete')
                break
            time.sleep(1)

    except KeyboardInterrupt:
        pass

@cli.command('status', short_help='Get the status of an operation')
@click.argument('operation-name')
@pass_context
def operation_status(context, operation_name):
    context.logger.info("Getting operation status...\n")
    stub = operations_pb2_grpc.OperationsStub(context.channel)

    request = operations_pb2.GetOperationRequest(name=operation_name)

    response = stub.GetOperation(request)
    _log_operation(context, response)

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
        _log_operation(context, op)

def _log_operation(context, operation):
    op_meta = ExecuteOperationMetadata()
    operation.metadata.Unpack(op_meta)

    context.logger.info("Name  : {}".format(operation.name))
    context.logger.info("Done  : {}".format(operation.done))
    context.logger.info("Stage : {}".format(ExecuteOperationMetadata.Stage.Name(op_meta.stage)))
    context.logger.info("Key   : {}".format(operation.response))
