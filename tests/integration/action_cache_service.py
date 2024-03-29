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


from unittest import mock

import grpc
from grpc._server import _Context
import pytest


from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.actioncache import service
from buildgrid.server.actioncache.instance import ActionCache
from buildgrid.server.actioncache.service import ActionCacheService


server = mock.create_autospec(grpc.server)


# Can mock this
@pytest.fixture
def context():
    yield mock.MagicMock(spec=_Context)


@pytest.fixture
def cas():
    yield lru_memory_cache.LRUMemoryCache(1024 * 1024)


@pytest.fixture
def cache_instances(cas):
    yield {"": ActionCache(cas, 50)}


def test_simple_action_result(cache_instances, context):
    with mock.patch.object(service, 'remote_execution_pb2_grpc'):
        ac_service = ActionCacheService(server)

    for k, v in cache_instances.items():
        ac_service.add_instance(k, v)

    action_digest = remote_execution_pb2.Digest(hash='sample', size_bytes=4)

    # Check that before adding the ActionResult, attempting to fetch it fails
    request = remote_execution_pb2.GetActionResultRequest(instance_name="",
                                                          action_digest=action_digest)
    ac_service.GetActionResult(request, context)
    context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)

    # Add an ActionResult to the cache
    action_result = remote_execution_pb2.ActionResult(stdout_raw=b'example output')
    request = remote_execution_pb2.UpdateActionResultRequest(action_digest=action_digest,
                                                             action_result=action_result)
    ac_service.UpdateActionResult(request, context)

    # Check that fetching it now works
    request = remote_execution_pb2.GetActionResultRequest(action_digest=action_digest)
    fetched_result = ac_service.GetActionResult(request, context)
    assert fetched_result.stdout_raw == action_result.stdout_raw


def test_disabled_update_action_result(context):
    disabled_push = ActionCache(cas, 50, False)
    with mock.patch.object(service, 'remote_execution_pb2_grpc'):
        ac_service = ActionCacheService(server)
        ac_service.add_instance("", disabled_push)

    request = remote_execution_pb2.UpdateActionResultRequest(instance_name='')
    ac_service.UpdateActionResult(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)


def test_disabled_cache_failed_actions(cas, context):
    disabled_failed_actions = ActionCache(cas, 50, True, False)
    with mock.patch.object(service, 'remote_execution_pb2_grpc'):
        ac_service = ActionCacheService(server)
        ac_service.add_instance("", disabled_failed_actions)

    failure_action_digest = remote_execution_pb2.Digest(hash='failure', size_bytes=4)

    # Add a non-zero exit code ActionResult to the cache
    action_result = remote_execution_pb2.ActionResult(stdout_raw=b'Failed', exit_code=1)
    request = remote_execution_pb2.UpdateActionResultRequest(action_digest=failure_action_digest,
                                                             action_result=action_result)
    ac_service.UpdateActionResult(request, context)

    # Check that before adding the ActionResult, attempting to fetch it fails
    request = remote_execution_pb2.GetActionResultRequest(instance_name="",
                                                          action_digest=failure_action_digest)
    ac_service.GetActionResult(request, context)
    context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)

    success_action_digest = remote_execution_pb2.Digest(hash='success', size_bytes=4)

    # Now add a zero exit code Action result to the cache, and check that fetching
    # it is successful
    success_action_result = remote_execution_pb2.ActionResult(stdout_raw=b'Successful')
    request = remote_execution_pb2.UpdateActionResultRequest(action_digest=success_action_digest,
                                                             action_result=success_action_result)
    ac_service.UpdateActionResult(request, context)
    request = remote_execution_pb2.GetActionResultRequest(instance_name="",
                                                          action_digest=success_action_digest)
    fetched_result = ac_service.GetActionResult(request, context)
    assert fetched_result.stdout_raw == success_action_result.stdout_raw
