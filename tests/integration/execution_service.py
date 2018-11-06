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

import uuid
from unittest import mock

import grpc
from grpc._server import _Context
import pytest

from buildgrid._enums import OperationStage
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.longrunning import operations_pb2

from buildgrid.utils import create_digest
from buildgrid.server import job
from buildgrid.server.controller import ExecutionController
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.actioncache.storage import ActionCache
from buildgrid.server.execution import service
from buildgrid.server.execution.service import ExecutionService


server = mock.create_autospec(grpc.server)
action = remote_execution_pb2.Action(do_not_cache=True)
action_digest = create_digest(action.SerializeToString())


@pytest.fixture
def context():
    cxt = mock.MagicMock(spec=_Context)
    yield cxt


@pytest.fixture(params=["action-cache", "no-action-cache"])
def controller(request):
    storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)
    write_session = storage.begin_write(action_digest)
    storage.commit_write(action_digest, write_session)

    if request.param == "action-cache":
        cache = ActionCache(storage, 50)
        yield ExecutionController(cache, storage)

    else:
        yield ExecutionController(None, storage)


# Instance to test
@pytest.fixture
def instance(controller):
    with mock.patch.object(service, 'remote_execution_pb2_grpc'):
        execution_service = ExecutionService(server)
        execution_service.add_instance("", controller.execution_instance)
        yield execution_service


@pytest.mark.parametrize("skip_cache_lookup", [True, False])
def test_execute(skip_cache_lookup, instance, context):
    request = remote_execution_pb2.ExecuteRequest(instance_name='',
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=skip_cache_lookup)
    response = instance.Execute(request, context)

    result = next(response)
    assert isinstance(result, operations_pb2.Operation)
    metadata = remote_execution_pb2.ExecuteOperationMetadata()
    result.metadata.Unpack(metadata)
    assert metadata.stage == OperationStage.QUEUED.value
    operation_uuid = result.name.split('/')[-1]
    assert uuid.UUID(operation_uuid, version=4)
    assert result.done is False


def test_wrong_execute_instance(instance, context):
    request = remote_execution_pb2.ExecuteRequest(instance_name='blade')
    response = instance.Execute(request, context)

    next(response)
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_no_action_digest_in_storage(instance, context):
    request = remote_execution_pb2.ExecuteRequest(instance_name='',
                                                  skip_cache_lookup=True)
    response = instance.Execute(request, context)

    next(response)
    context.set_code.assert_called_once_with(grpc.StatusCode.FAILED_PRECONDITION)


def test_wait_execution(instance, controller, context):
    job_name = controller.execution_instance._scheduler.queue_job(action,
                                                                  action_digest,
                                                                  skip_cache_lookup=True)

    controller.execution_instance._scheduler._update_job_operation_stage(job_name,
                                                                         OperationStage.COMPLETED)

    request = remote_execution_pb2.WaitExecutionRequest(name=job_name)

    response = instance.WaitExecution(request, context)

    result = next(response)

    assert isinstance(result, operations_pb2.Operation)
    metadata = remote_execution_pb2.ExecuteOperationMetadata()
    result.metadata.Unpack(metadata)
    assert metadata.stage == job.OperationStage.COMPLETED.value
    assert result.done is True


def test_wrong_instance_wait_execution(instance, context):
    request = remote_execution_pb2.WaitExecutionRequest(name="blade")
    next(instance.WaitExecution(request, context))

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
