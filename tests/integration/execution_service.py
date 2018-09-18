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

from google.protobuf import any_pb2
import grpc
from grpc._server import _Context
import pytest

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.longrunning import operations_pb2

from buildgrid.server import job
from buildgrid.server.controller import ExecutionController
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.actioncache.storage import ActionCache
from buildgrid.server.execution import service
from buildgrid.server.execution.service import ExecutionService


server = mock.create_autospec(grpc.server)


@pytest.fixture
def context():
    cxt = mock.MagicMock(spec=_Context)
    yield cxt


@pytest.fixture(params=["action-cache", "no-action-cache"])
def controller(request):
    if request.param == "action-cache":
        storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)
        cache = ActionCache(storage, 50)
        yield ExecutionController(cache, storage)
    else:
        yield ExecutionController()


# Instance to test
@pytest.fixture
def instance(controller):
    with mock.patch.object(service, 'remote_execution_pb2_grpc'):
        execution_service = ExecutionService(server)
        execution_service.add_instance("", controller.execution_instance)
        yield execution_service


@pytest.mark.parametrize("skip_cache_lookup", [True, False])
def test_execute(skip_cache_lookup, instance, context):
    action_digest = remote_execution_pb2.Digest()
    action_digest.hash = 'zhora'

    request = remote_execution_pb2.ExecuteRequest(instance_name='',
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=skip_cache_lookup)
    response = instance.Execute(request, context)

    result = next(response)
    assert isinstance(result, operations_pb2.Operation)
    metadata = remote_execution_pb2.ExecuteOperationMetadata()
    result.metadata.Unpack(metadata)
    assert metadata.stage == job.ExecuteStage.QUEUED.value
    assert uuid.UUID(result.name, version=4)
    assert result.done is False


def test_wrong_execute_instance(instance, context):
    request = remote_execution_pb2.ExecuteRequest(instance_name='blade')
    response = instance.Execute(request, context)

    next(response)
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_wait_execution(instance, controller, context):
    action_digest = remote_execution_pb2.Digest()
    action_digest.hash = 'zhora'

    j = job.Job(action_digest, None)
    j._operation.done = True

    request = remote_execution_pb2.WaitExecutionRequest(name="{}/{}".format('', j.name))

    controller.execution_instance._scheduler.jobs[j.name] = j

    action_result_any = any_pb2.Any()
    action_result = remote_execution_pb2.ActionResult()
    action_result_any.Pack(action_result)

    j.update_execute_stage(job.ExecuteStage.COMPLETED)

    response = instance.WaitExecution(request, context)

    result = next(response)

    assert isinstance(result, operations_pb2.Operation)
    metadata = remote_execution_pb2.ExecuteOperationMetadata()
    result.metadata.Unpack(metadata)
    assert metadata.stage == job.ExecuteStage.COMPLETED.value
    assert uuid.UUID(result.name, version=4)
    assert result.done is True


def test_wrong_instance_wait_execution(instance, context):
    request = remote_execution_pb2.WaitExecutionRequest(name="blade")
    next(instance.WaitExecution(request, context))

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
