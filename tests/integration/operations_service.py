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
#
# pylint: disable=redefined-outer-name


import queue
from unittest import mock

from google.protobuf import any_pb2
import grpc
from grpc._server import _Context
import pytest

from buildgrid._enums import OperationStage
from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.controller import ExecutionController
from buildgrid.server.operations import service
from buildgrid.server.operations.service import OperationsService
from buildgrid.utils import create_digest


server = mock.create_autospec(grpc.server)
instance_name = "blade"

command = remote_execution_pb2.Command()
command_digest = create_digest(command.SerializeToString())

action = remote_execution_pb2.Action(command_digest=command_digest,
                                     do_not_cache=True)
action_digest = create_digest(action.SerializeToString())


# Can mock this
@pytest.fixture
def context():
    yield mock.MagicMock(spec=_Context)


# Requests to make
@pytest.fixture
def execute_request():
    yield remote_execution_pb2.ExecuteRequest(instance_name='',
                                              action_digest=action_digest,
                                              skip_cache_lookup=True)


@pytest.fixture
def controller():
    storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)

    write_session = storage.begin_write(command_digest)
    write_session.write(command.SerializeToString())
    storage.commit_write(command_digest, write_session)

    write_session = storage.begin_write(action_digest)
    write_session.write(action.SerializeToString())
    storage.commit_write(action_digest, write_session)

    yield ExecutionController(None, storage)


# Instance to test
@pytest.fixture
def instance(controller):
    with mock.patch.object(service, 'operations_pb2_grpc'):
        operation_service = OperationsService(server)
        operation_service.add_instance(instance_name, controller.operations_instance)

        yield operation_service


# Blank instance
@pytest.fixture
def blank_instance(controller):
    with mock.patch.object(service, 'operations_pb2_grpc'):
        operation_service = OperationsService(server)
        operation_service.add_instance('', controller.operations_instance)

        yield operation_service


# Queue an execution, get operation corresponding to that request
def test_get_operation(instance, controller, execute_request, context):
    job_name = controller.execution_instance.execute(execute_request.action_digest,
                                                     execute_request.skip_cache_lookup)

    message_queue = queue.Queue()
    operation_name = controller.execution_instance.register_job_peer(job_name,
                                                                     context.peer(),
                                                                     message_queue)

    request = operations_pb2.GetOperationRequest()

    # The execution instance name is normally set in add_instance, but since
    # we're manually creating the instance here, it doesn't get a name.
    # Therefore we need to manually add the instance name to the operation
    # name in the GetOperation request.
    request.name = "{}/{}".format(instance_name, operation_name)

    response = instance.GetOperation(request, context)
    assert response.name == "{}/{}".format(instance_name, operation_name)


# Queue an execution, get operation corresponding to that request
def test_get_operation_blank(blank_instance, controller, execute_request, context):
    job_name = controller.execution_instance.execute(execute_request.action_digest,
                                                     execute_request.skip_cache_lookup)

    message_queue = queue.Queue()
    operation_name = controller.execution_instance.register_job_peer(job_name,
                                                                     context.peer(),
                                                                     message_queue)

    request = operations_pb2.GetOperationRequest()

    request.name = operation_name

    response = blank_instance.GetOperation(request, context)
    assert response.name == operation_name


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
    job_name = controller.execution_instance.execute(execute_request.action_digest,
                                                     execute_request.skip_cache_lookup)

    message_queue = queue.Queue()
    operation_name = controller.execution_instance.register_job_peer(job_name,
                                                                     context.peer(),
                                                                     message_queue)

    request = operations_pb2.ListOperationsRequest(name=instance_name)
    response = instance.ListOperations(request, context)

    names = response.operations[0].name.split('/')
    assert names[0] == instance_name
    assert names[1] == operation_name


def test_list_operations_blank(blank_instance, controller, execute_request, context):
    job_name = controller.execution_instance.execute(execute_request.action_digest,
                                                     execute_request.skip_cache_lookup)

    message_queue = queue.Queue()
    operation_name = controller.execution_instance.register_job_peer(job_name,
                                                                     context.peer(),
                                                                     message_queue)

    request = operations_pb2.ListOperationsRequest(name='')
    response = blank_instance.ListOperations(request, context)

    assert response.operations[0].name.split('/')[-1] == operation_name


def test_list_operations_instance_fail(instance, controller, execute_request, context):
    controller.execution_instance.execute(execute_request.action_digest,
                                          execute_request.skip_cache_lookup)

    request = operations_pb2.ListOperationsRequest()
    instance.ListOperations(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_list_operations_empty(instance, context):
    request = operations_pb2.ListOperationsRequest(name=instance_name)

    response = instance.ListOperations(request, context)

    assert not response.operations


# Send execution off, delete, try to find operation should fail
def test_delete_operation(instance, controller, execute_request, context):
    job_name = controller.execution_instance.execute(execute_request.action_digest,
                                                     execute_request.skip_cache_lookup)

    message_queue = queue.Queue()
    operation_name = controller.execution_instance.register_job_peer(job_name,
                                                                     context.peer(),
                                                                     message_queue)

    request = operations_pb2.DeleteOperationRequest()
    request.name = operation_name
    instance.DeleteOperation(request, context)

    request_name = "{}/{}".format(instance_name, operation_name)

    with pytest.raises(InvalidArgumentError):
        controller.operations_instance.get_operation(request_name)


# Send execution off, delete, try to find operation should fail
def test_delete_operation_blank(blank_instance, controller, execute_request, context):
    request = operations_pb2.DeleteOperationRequest()
    request.name = "runner"
    blank_instance.DeleteOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_delete_operation_fail(instance, context):
    request = operations_pb2.DeleteOperationRequest()
    request.name = "{}/{}".format(instance_name, "runner")
    instance.DeleteOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_cancel_operation(instance, controller, execute_request, context):
    job_name = controller.execution_instance.execute(execute_request.action_digest,
                                                     execute_request.skip_cache_lookup)

    message_queue = queue.Queue()
    operation_name = controller.execution_instance.register_job_peer(job_name,
                                                                     context.peer(),
                                                                     message_queue)

    request = operations_pb2.CancelOperationRequest()
    request.name = "{}/{}".format(instance_name, operation_name)

    instance.CancelOperation(request, context)

    request = operations_pb2.ListOperationsRequest(name=instance_name)
    response = instance.ListOperations(request, context)

    assert len(response.operations) == 1

    for operation in response.operations:
        operation_metadata = remote_execution_pb2.ExecuteOperationMetadata()
        operation.metadata.Unpack(operation_metadata)
        assert operation_metadata.stage == OperationStage.COMPLETED.value


def test_cancel_operation_blank(blank_instance, context):
    request = operations_pb2.CancelOperationRequest()
    request.name = "runner"
    blank_instance.CancelOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_cancel_operation__fail(instance, context):
    request = operations_pb2.CancelOperationRequest()
    instance.CancelOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def _pack_any(pack):
    some_any = any_pb2.Any()
    some_any.Pack(pack)
    return some_any
