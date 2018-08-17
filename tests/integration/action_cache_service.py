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

import grpc
from grpc._server import _Context
import pytest


from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.execution import action_cache, action_cache_service


# Can mock this
@pytest.fixture
def context():
    yield mock.MagicMock(spec=_Context)


@pytest.fixture
def cas():
    yield lru_memory_cache.LRUMemoryCache(1024 * 1024)


@pytest.fixture
def cache(cas):
    yield action_cache.ActionCache(cas, 50)


def test_simple_action_result(cache, context):
    service = action_cache_service.ActionCacheService(cache)
    action_digest = remote_execution_pb2.Digest(hash='sample', size_bytes=4)

    # Check that before adding the ActionResult, attempting to fetch it fails
    request = remote_execution_pb2.GetActionResultRequest(action_digest=action_digest)
    service.GetActionResult(request, context)
    context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)

    # Add an ActionResult to the cache
    action_result = remote_execution_pb2.ActionResult(stdout_raw=b'example output')
    request = remote_execution_pb2.UpdateActionResultRequest(action_digest=action_digest,
                                                             action_result=action_result)
    service.UpdateActionResult(request, context)

    # Check that fetching it now works
    request = remote_execution_pb2.GetActionResultRequest(action_digest=action_digest)
    fetched_result = service.GetActionResult(request, context)
    assert fetched_result.stdout_raw == action_result.stdout_raw


def test_disabled_update_action_result(cache, context):
    disabled_push = action_cache.ActionCache(cas, 50, False)
    service = action_cache_service.ActionCacheService(disabled_push)

    request = remote_execution_pb2.UpdateActionResultRequest()
    service.UpdateActionResult(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)
