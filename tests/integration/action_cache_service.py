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
from buildgrid._protos.google.devtools.remoteexecution.v1test import remote_execution_pb2

from buildgrid.server import scheduler
from buildgrid.server.execution import execution_instance, action_cache_service

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
    yield action_cache_service.ActionCacheService(execution)

def test_get_action_result(instance, context):
    request = remote_execution_pb2.GetActionResultRequest()
    instance.GetActionResult(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)

def test_update_action_result(instance, context):
    request = remote_execution_pb2.UpdateActionResultRequest()
    instance.UpdateActionResult(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)
