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

import uuid
from unittest import mock

from grpc._server import _Context
import pytest

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid.server import action_cache, scheduler, job
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.execution import execution_instance, execution_service


@pytest.fixture
def context():
    cxt = mock.MagicMock(spec=_Context)
    yield cxt


@pytest.fixture(params=["action-cache", "no-action-cache"])
def execution(request):
    if request.param == "action-cache":
        storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)
        cache = action_cache.ActionCache(storage, 50)
        schedule = scheduler.Scheduler(cache)
        return execution_instance.ExecutionInstance(schedule, storage)
    return execution_instance.ExecutionInstance(scheduler.Scheduler())


# Instance to test
@pytest.fixture
def instance(execution):
    yield execution_service.ExecutionService(execution)


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

"""
def test_wait_execution(instance, context):
    # TODO: Figure out why next(response) hangs on the .get()
    # method when running in pytest.
    action_digest = remote_execution_pb2.Digest()
    action_digest.hash = 'zhora'

    j = job.Job(action_digest, None)
    j._operation.done = True

    request = remote_execution_pb2.WaitExecutionRequest(name=j.name)

    instance._instance._scheduler.jobs[j.name] = j

    action_result_any = any_pb2.Any()
    action_result = remote_execution_pb2.ActionResult()
    action_result_any.Pack(action_result)

    instance._instance._scheduler._update_execute_stage(j, job.ExecuteStage.COMPLETED)

    response = instance.WaitExecution(request, context)
"""
