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

import errno
import logging
import stat
import os
import click
import grpc

from buildgrid.utils import merkle_maker, create_digest, write_fetch_blob
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc
from buildgrid._protos.google.bytestream import bytestream_pb2_grpc
from buildgrid._protos.google.longrunning import operations_pb2, operations_pb2_grpc

from ..cli import pass_context


@click.group(name='execute', short_help="Execute simple operations.")
@click.option('--port', type=click.INT, default='50051', show_default=True,
              help="Remote server's port number.")
@click.option('--host', type=click.STRING, default='localhost', show_default=True,
              help="Remote server's hostname.")
@pass_context
def cli(context, host, port):
    context.logger = logging.getLogger(__name__)
    context.logger.info("Starting on port {}".format(port))

    context.channel = grpc.insecure_channel('{}:{}'.format(host, port))
    context.port = port


@cli.command('request-dummy', short_help="Send a dummy action.")
@click.option('--instance-name', type=click.STRING, default='testing', show_default=True,
              help="Targeted farm instance name.")
@click.option('--number', type=click.INT, default=1, show_default=True,
              help="Number of request to send.")
@click.option('--wait-for-completion', is_flag=True,
              help="Stream updates until jobs are completed.")
@pass_context
def request_dummy(context, number, instance_name, wait_for_completion):
    action_digest = remote_execution_pb2.Digest()

    context.logger.info("Sending execution request...")
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)

    request = remote_execution_pb2.ExecuteRequest(instance_name=instance_name,
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=True)

    responses = list()
    for _ in range(0, number):
        responses.append(stub.Execute(request))

    for response in responses:
        if wait_for_completion:
            for stream in response:
                context.logger.info(stream)
        else:
            context.logger.info(next(response))


@cli.command('status', short_help="Get the status of an operation.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@pass_context
def operation_status(context, operation_name):
    context.logger.info("Getting operation status...")
    stub = operations_pb2_grpc.OperationsStub(context.channel)

    request = operations_pb2.GetOperationRequest(name=operation_name)

    response = stub.GetOperation(request)
    context.logger.info(response)


@cli.command('list', short_help="List operations.")
@pass_context
def list_operations(context):
    context.logger.info("Getting list of operations")
    stub = operations_pb2_grpc.OperationsStub(context.channel)

    request = operations_pb2.ListOperationsRequest()

    response = stub.ListOperations(request)

    if not response.operations:
        context.logger.warning("No operations to list")
        return

    for op in response.operations:
        context.logger.info(op)


@cli.command('wait', short_help="Streams an operation until it is complete.")
@click.argument('operation-name', nargs=1, type=click.STRING, required=True)
@pass_context
def wait_execution(context, operation_name):
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)
    request = remote_execution_pb2.WaitExecutionRequest(name=operation_name)

    response = stub.WaitExecution(request)

    for stream in response:
        context.logger.info(stream)


@cli.command('command', short_help="Send a command to be executed.")
@click.option('--instance-name', type=click.STRING, default='testing', show_default=True,
              help="Targeted farm instance name.")
@click.option('--output-file', nargs=2, type=(click.STRING, click.BOOL), multiple=True,
              help="Tuple of expected output file and is-executeable flag.")
@click.option('--output-directory', default='testing', show_default=True,
              help="Output directory for the output files.")
@click.argument('input-root', nargs=1, type=click.Path(), required=True)
@click.argument('commands', nargs=-1, type=click.STRING, required=True)
@pass_context
def command(context, input_root, commands, output_file, output_directory, instance_name):
    stub = remote_execution_pb2_grpc.ExecutionStub(context.channel)

    execute_command = remote_execution_pb2.Command()

    for arg in commands:
        execute_command.arguments.extend([arg])

    output_executeables = []
    for file, is_executeable in output_file:
        execute_command.output_files.extend([file])
        if is_executeable:
            output_executeables.append(file)

    command_digest = create_digest(execute_command.SerializeToString())
    context.logger.info(command_digest)

    # TODO: Check for missing blobs
    digest = None
    for _, digest in merkle_maker(input_root):
        pass

    action = remote_execution_pb2.Action(command_digest=command_digest,
                                         input_root_digest=digest,
                                         do_not_cache=True)

    action_digest = create_digest(action.SerializeToString())

    context.logger.info("Sending execution request...")

    requests = []
    requests.append(remote_execution_pb2.BatchUpdateBlobsRequest.Request(
        digest=command_digest, data=execute_command.SerializeToString()))

    requests.append(remote_execution_pb2.BatchUpdateBlobsRequest.Request(
        digest=action_digest, data=action.SerializeToString()))

    request = remote_execution_pb2.BatchUpdateBlobsRequest(instance_name=instance_name,
                                                           requests=requests)
    remote_execution_pb2_grpc.ContentAddressableStorageStub(context.channel).BatchUpdateBlobs(request)

    request = remote_execution_pb2.ExecuteRequest(instance_name=instance_name,
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=True)
    response = stub.Execute(request)

    stub = bytestream_pb2_grpc.ByteStreamStub(context.channel)

    stream = None
    for stream in response:
        context.logger.info(stream)

    execute_response = remote_execution_pb2.ExecuteResponse()
    stream.response.Unpack(execute_response)

    for output_file_response in execute_response.result.output_files:
        path = os.path.join(output_directory, output_file_response.path)

        if not os.path.exists(os.path.dirname(path)):

            try:
                os.makedirs(os.path.dirname(path))

            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

        with open(path, 'wb+') as f:
            write_fetch_blob(f, stub, output_file_response.digest, instance_name)

        if output_file_response.path in output_executeables:
            st = os.stat(path)
            os.chmod(path, st.st_mode | stat.S_IXUSR)
