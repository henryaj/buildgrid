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

import grpc
import pytest

from unittest import mock

from grpc._server import _Context
from google.devtools.remoteexecution.v1test import remote_execution_pb2
from google.longrunning import operations_pb2

from buildgrid.server import scheduler
from buildgrid.server.execution._exceptions import InvalidArgumentError
from buildgrid.server.execution import execution_instance, operations_service

# Can mock this
@pytest.fixture
def context():
    yield mock.MagicMock(spec = _Context)

# Requests to make
@pytest.fixture
def execute_request():
    action = remote_execution_pb2.Action()
    action.command_digest.hash = 'zhora'

    yield remote_execution_pb2.ExecuteRequest(instance_name = '',
                                              action = action,
                                              skip_cache_lookup = True)

@pytest.fixture
def schedule():
    yield scheduler.Scheduler()

@pytest.fixture
def execution(schedule):
    yield execution_instance.ExecutionInstance(schedule)

# Instance to test
@pytest.fixture
def instance(execution):
    yield operations_service.OperationsService(execution)

# Queue an execution, get operation corresponding to that request
def test_get_operation(instance, execute_request, context):
    response_execute = instance._instance.execute(execute_request.action,
                                                  execute_request.skip_cache_lookup)

    request = operations_pb2.GetOperationRequest()
    request.name = response_execute.name

    response = instance.GetOperation(request, context)
    assert response is response_execute

def test_get_operation_fail(instance, context):
    request = operations_pb2.GetOperationRequest()
    response = instance.GetOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)

# Send execution off, delete, try to find operation should fail
def test_delete_operation(instance, execute_request, context):
    response_execute = instance._instance.execute(execute_request.action,
                                                  execute_request.skip_cache_lookup)
    request = operations_pb2.DeleteOperationRequest()
    request.name = response_execute.name
    response = instance.DeleteOperation(request, context)

    request = operations_pb2.GetOperationRequest()
    request.name = response_execute.name
    with pytest.raises(InvalidArgumentError):
        instance._instance.get_operation(response_execute.name)

def test_delete_operation_fail(instance, execute_request, context):
    request = operations_pb2.DeleteOperationRequest()
    instance.DeleteOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)

def test_cancel_operation(instance, context):
    request = operations_pb2.CancelOperationRequest()
    instance.CancelOperation(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)
