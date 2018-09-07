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

# pylint: disable=redefined-outer-name

from unittest import mock

from google.protobuf import any_pb2
import grpc
from grpc._server import _Context
import pytest

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.longrunning import operations_pb2

from buildgrid.server.controller import ExecutionController
from buildgrid.server._exceptions import InvalidArgumentError

from buildgrid.server.operations import service
from buildgrid.server.operations.service import OperationsService


server = mock.create_autospec(grpc.server)
instance_name = "blade"


# Can mock this
@pytest.fixture
def context():
    yield mock.MagicMock(spec=_Context)


# Requests to make
@pytest.fixture
def execute_request():
    action_digest = remote_execution_pb2.Digest()
    action_digest.hash = 'zhora'

    yield remote_execution_pb2.ExecuteRequest(instance_name='',
                                              action_digest=action_digest,
                                              skip_cache_lookup=True)


@pytest.fixture
def controller():
    yield ExecutionController()


# Instance to test
@pytest.fixture
def instance(controller):
    instances = {instance_name: controller.operations_instance}
    with mock.patch.object(service, 'operations_pb2_grpc'):
        yield OperationsService(server, instances)


# Queue an execution, get operation corresponding to that request
def test_get_operation(instance, controller, execute_request, context):
    response_execute = controller.execution_instance.execute(execute_request.action_digest,
                                                             execute_request.skip_cache_lookup)

    request = operations_pb2.GetOperationRequest()

    request.name = "{}/{}".format(instance_name, response_execute.name)

    response = instance.GetOperation(request, context)
    assert response is response_execute


def test_get_operation_fail(instance, context):
    request = operations_pb2.GetOperationRequest()
    request.name = "{}/{}".format(instance_name, "runner")
    instance.GetOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_get_operation_instance_fail(instance, context):
    request = operations_pb2.GetOperationRequest()
    instance.GetOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_list_operations(instance, controller, execute_request, context):
    response_execute = controller.execution_instance.execute(execute_request.action_digest,
                                                             execute_request.skip_cache_lookup)

    request = operations_pb2.ListOperationsRequest(name=instance_name)
    response = instance.ListOperations(request, context)

    assert response.operations[0].name.split('/')[-1] == response_execute.name


def test_list_operations_instance_fail(instance, controller, execute_request, context):
    controller.execution_instance.execute(execute_request.action_digest,
                                          execute_request.skip_cache_lookup)

    request = operations_pb2.ListOperationsRequest()
    instance.ListOperations(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_list_operations_with_result(instance, controller, execute_request, context):
    response_execute = controller.execution_instance.execute(execute_request.action_digest,
                                                             execute_request.skip_cache_lookup)

    action_result = remote_execution_pb2.ActionResult()
    output_file = remote_execution_pb2.OutputFile(path='unicorn')
    action_result.output_files.extend([output_file])

    controller.operations_instance._scheduler.job_complete(response_execute.name,
                                                           _pack_any(action_result))

    request = operations_pb2.ListOperationsRequest(name=instance_name)
    response = instance.ListOperations(request, context)

    assert response.operations[0].name.split('/')[-1] == response_execute.name

    execute_response = remote_execution_pb2.ExecuteResponse()
    response.operations[0].response.Unpack(execute_response)
    assert execute_response.result == action_result


def test_list_operations_empty(instance, context):
    request = operations_pb2.ListOperationsRequest(name=instance_name)

    response = instance.ListOperations(request, context)

    assert len(response.operations) is 0


# Send execution off, delete, try to find operation should fail
def test_delete_operation(instance, controller, execute_request, context):
    response_execute = controller.execution_instance.execute(execute_request.action_digest,
                                                             execute_request.skip_cache_lookup)
    request = operations_pb2.DeleteOperationRequest()
    request.name = "{}/{}".format(instance_name, response_execute.name)
    instance.DeleteOperation(request, context)

    request = operations_pb2.GetOperationRequest()
    request.name = "{}/{}".format(instance_name, response_execute.name)

    with pytest.raises(InvalidArgumentError):
        controller.operations_instance.get_operation(response_execute.name)


def test_delete_operation_fail(instance, context):
    request = operations_pb2.DeleteOperationRequest()
    request.name = "{}/{}".format(instance_name, "runner")
    instance.DeleteOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_cancel_operation(instance, context):
    request = operations_pb2.CancelOperationRequest()
    request.name = "{}/{}".format(instance_name, "runner")
    instance.CancelOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)


def test_cancel_operation_instance_fail(instance, context):
    request = operations_pb2.CancelOperationRequest()
    instance.CancelOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def _pack_any(pack):
    some_any = any_pb2.Any()
    some_any.Pack(pack)
    return some_any
