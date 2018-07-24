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

import copy
import grpc
import pytest
import uuid

from unittest import mock

from grpc._server import _Context
from google.devtools.remoteexecution.v1test import remote_execution_pb2
from google.devtools.remoteworkers.v1test2 import bots_pb2, worker_pb2
from google.protobuf import any_pb2

from buildgrid.server import scheduler, job
from buildgrid.server.job import ExecuteStage, LeaseState
from buildgrid.server.worker import bots_interface, bots_service

# GRPC context
@pytest.fixture
def context():
    yield mock.MagicMock(spec = _Context)

@pytest.fixture
def bot_session():
    bot = bots_pb2.BotSession()
    bot.bot_id = 'ana'
    yield bot

@pytest.fixture
def schedule():
    yield scheduler.Scheduler()

@pytest.fixture
def bots(schedule):
    yield bots_interface.BotsInterface(schedule)

# Instance to test
@pytest.fixture
def instance(bots):
    yield bots_service.BotsService(bots)

def test_create_bot_session(bot_session, context, instance):
    parent = 'rach'
    request = bots_pb2.CreateBotSessionRequest(parent=parent,
                                               bot_session=bot_session)

    response = instance.CreateBotSession(request, context)

    assert isinstance(response, bots_pb2.BotSession)
    assert uuid.UUID(response.name, version=4)
    assert bot_session.bot_id == response.bot_id

def test_create_bot_session_bot_id_fail(context, instance):
    bot = bots_pb2.BotSession()

    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot)

    instance.CreateBotSession(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)

def test_update_bot_session(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    bot = instance.CreateBotSession(request, context)

    request = bots_pb2.UpdateBotSessionRequest(name=bot.name,
                                               bot_session=bot)

    response = instance.UpdateBotSession(request, context)

    assert isinstance(response, bots_pb2.BotSession)
    assert bot == response

def test_update_bot_session_zombie(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    bot = instance.CreateBotSession(request, context)
    # Update server with incorrect UUID by rotating it
    bot.name = bot.name[len(bot.name) : 0]

    request = bots_pb2.UpdateBotSessionRequest(name=bot.name,
                                               bot_session=bot)

    response = instance.UpdateBotSession(request, context)

    assert isinstance(response, bots_pb2.BotSession)
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)

def test_update_bot_session_bot_id_fail(bot_session, context, instance):
    name='ana'
    request = bots_pb2.UpdateBotSessionRequest(name=name,
                                               bot_session=bot_session)

    instance.UpdateBotSession(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)

@pytest.mark.parametrize("number_of_leases", [1, 3, 500])
def test_update_leases(number_of_leases, bot_session, context, instance):
    leases = [bots_pb2.Lease() for x in range(number_of_leases)]
    bot_session.leases.extend(leases)
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    # Simulated the severed binding between client and server
    bot = copy.deepcopy(instance.CreateBotSession(request, context))

    request = bots_pb2.UpdateBotSessionRequest(name=bot.name,
                                               bot_session=bot)

    response = instance.UpdateBotSession(request, context)

    assert isinstance(response, bots_pb2.BotSession)
    assert len(response.leases) == len(bot.leases)
    assert bot == response

def test_update_leases_with_work(bot_session, context, instance):
    leases = [bots_pb2.Lease() for x in range(2)]
    bot_session.leases.extend(leases)

    # Inject some work to be done
    action = remote_execution_pb2.Action()
    action.command_digest.hash = 'rick'
    instance._instance._scheduler.append_job(job.Job(action))

    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    # Simulated the severed binding between client and server
    bot = copy.deepcopy(instance.CreateBotSession(request, context))

    request = bots_pb2.UpdateBotSessionRequest(name=bot.name,
                                               bot_session=bot)

    response = instance.UpdateBotSession(request, context)
    response_action = remote_execution_pb2.Action()
    _unpack_any(response.leases[0].inline_assignment, response_action)

    assert isinstance(response, bots_pb2.BotSession)
    assert response.leases[0].state == LeaseState.PENDING.value
    assert response.leases[1].state == LeaseState.LEASE_STATE_UNSPECIFIED.value
    assert uuid.UUID(response.leases[0].assignment, version=4)
    assert response_action == action

def test_update_leases_work_complete(bot_session, context, instance):
    leases = [bots_pb2.Lease() for x in range(2)]
    bot_session.leases.extend(leases)

    # Inject some work to be done
    action = remote_execution_pb2.Action()
    action.command_digest.hash = 'rick'
    instance._instance._scheduler.append_job(job.Job(action))

    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    # Simulated the severed binding between client and server
    bot = copy.deepcopy(instance.CreateBotSession(request, context))

    request = bots_pb2.UpdateBotSessionRequest(name=bot.name,
                                               bot_session=bot)

    response = copy.deepcopy(instance.UpdateBotSession(request, context))

    operation_name = response.leases[0].assignment

    assert response.leases[0].state == LeaseState.PENDING.value
    response.leases[0].state = LeaseState.COMPLETED.value

    request = bots_pb2.UpdateBotSessionRequest(name=response.name,
                                               bot_session=response)
    # Simulated the severed binding between client and server
    response = copy.deepcopy(instance.UpdateBotSession(request, context))
    assert isinstance(response, bots_pb2.BotSession)
    assert instance._instance._scheduler.jobs[operation_name].execute_stage == ExecuteStage.COMPLETED

def test_post_bot_event_temp(context, instance):
    request = bots_pb2.PostBotEventTempRequest()
    instance.PostBotEventTemp(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)

def _unpack_any(unpack_from, to):
    any = any_pb2.Any()
    any.CopyFrom(unpack_from)
    any.Unpack(to)
    return to
