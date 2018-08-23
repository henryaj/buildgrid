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

import copy
from unittest import mock

import grpc
from grpc._server import _Context
import pytest

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid.server import job
from buildgrid.server.instance import BuildGridInstance
from buildgrid.server.job import LeaseState
from buildgrid.server.bots.instance import BotsInterface
from buildgrid.server.bots.service import BotsService


# GRPC context
@pytest.fixture
def context():
    yield mock.MagicMock(spec=_Context)


@pytest.fixture
def action_job():
    action_digest = remote_execution_pb2.Digest()
    j = job.Job(action_digest, None)
    yield j


@pytest.fixture
def bot_session():
    bot = bots_pb2.BotSession()
    bot.bot_id = 'ana'
    yield bot


@pytest.fixture
def buildgrid():
    yield BuildGridInstance()


@pytest.fixture
def bots(schedule):
    yield BotsInterface(schedule)


# Instance to test
@pytest.fixture
def instance(buildgrid):
    instances = {"": buildgrid}
    yield BotsService(instances)


def test_create_bot_session(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(bot_session=bot_session)

    response = instance.CreateBotSession(request, context)

    assert isinstance(response, bots_pb2.BotSession)
    assert bot_session.bot_id == response.bot_id


def test_create_bot_session_bot_id_fail(context, instance):
    bot = bots_pb2.BotSession()

    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot)

    instance.CreateBotSession(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_update_bot_session(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(bot_session=bot_session)
    bot = instance.CreateBotSession(request, context)

    request = bots_pb2.UpdateBotSessionRequest(name=bot.name,
                                               bot_session=bot)

    response = instance.UpdateBotSession(request, context)

    assert isinstance(response, bots_pb2.BotSession)
    assert bot == response


def test_update_bot_session_zombie(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(bot_session=bot_session)
    bot = instance.CreateBotSession(request, context)
    # Update server with incorrect UUID by rotating it
    bot.name = bot.name[len(bot.name): 0]

    request = bots_pb2.UpdateBotSessionRequest(name=bot.name,
                                               bot_session=bot)

    instance.UpdateBotSession(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_update_bot_session_bot_id_fail(bot_session, context, instance):
    request = bots_pb2.UpdateBotSessionRequest(bot_session=bot_session)

    instance.UpdateBotSession(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


@pytest.mark.parametrize("number_of_jobs", [0, 1, 3, 500])
def test_number_of_leases(number_of_jobs, bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(bot_session=bot_session)
    # Inject work
    for _ in range(0, number_of_jobs):
        action_digest = remote_execution_pb2.Digest()
        instance._instances[""].execute(action_digest, True)

    response = instance.CreateBotSession(request, context)

    assert len(response.leases) == number_of_jobs


def test_update_leases_with_work(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    # Inject work
    action_digest = remote_execution_pb2.Digest(hash='gaff')
    instance._instances[""].execute(action_digest, True)

    response = instance.CreateBotSession(request, context)

    assert len(response.leases) == 1
    response_action = remote_execution_pb2.Digest()
    response.leases[0].payload.Unpack(response_action)

    assert isinstance(response, bots_pb2.BotSession)
    assert response.leases[0].state == LeaseState.PENDING.value
    assert response_action == action_digest


def test_update_leases_work_complete(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    # Create bot session
    # Simulated the severed binding between client and server
    response = copy.deepcopy(instance.CreateBotSession(request, context))

    # Inject work
    action_digest = remote_execution_pb2.Digest(hash='gaff')
    instance._instances[""].execute(action_digest, True)

    request = bots_pb2.UpdateBotSessionRequest(name=response.name,
                                               bot_session=response)
    response = copy.deepcopy(instance.UpdateBotSession(request, context))

    assert response.leases[0].state == LeaseState.PENDING.value
    response.leases[0].state = LeaseState.ACTIVE.value

    request = bots_pb2.UpdateBotSessionRequest(name=response.name,
                                               bot_session=response)

    response = copy.deepcopy(instance.UpdateBotSession(request, context))

    response.leases[0].state = LeaseState.COMPLETED.value

    request = bots_pb2.UpdateBotSessionRequest(name=response.name,
                                               bot_session=response)
    response = copy.deepcopy(instance.UpdateBotSession(request, context))

    assert len(response.leases) is 0


def test_work_rejected_by_bot(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    # Inject work
    action_digest = remote_execution_pb2.Digest(hash='gaff')
    instance._instances[""].execute(action_digest, True)

    # Simulated the severed binding between client and server
    response = copy.deepcopy(instance.CreateBotSession(request, context))

    # Reject work
    assert response.leases[0].state == LeaseState.PENDING.value
    response.leases[0].state = LeaseState.COMPLETED.value
    request = bots_pb2.UpdateBotSessionRequest(name=response.name,
                                               bot_session=response)

    response = instance.UpdateBotSession(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)


@pytest.mark.parametrize("state", [LeaseState.LEASE_STATE_UNSPECIFIED, LeaseState.PENDING])
def test_work_out_of_sync_from_pending(state, bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    # Inject work
    action_digest = remote_execution_pb2.Digest(hash='gaff')
    instance._instances[""].execute(action_digest, True)

    # Simulated the severed binding between client and server
    response = copy.deepcopy(instance.CreateBotSession(request, context))

    response.leases[0].state = state.value

    request = bots_pb2.UpdateBotSessionRequest(name=response.name,
                                               bot_session=response)

    response = instance.UpdateBotSession(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.DATA_LOSS)


@pytest.mark.parametrize("state", [LeaseState.LEASE_STATE_UNSPECIFIED, LeaseState.PENDING])
def test_work_out_of_sync_from_active(state, bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    # Inject work
    action_digest = remote_execution_pb2.Digest(hash='gaff')
    instance._instances[""].execute(action_digest, True)

    # Simulated the severed binding between client and server
    response = copy.deepcopy(instance.CreateBotSession(request, context))

    response.leases[0].state = LeaseState.ACTIVE.value

    request = copy.deepcopy(bots_pb2.UpdateBotSessionRequest(name=response.name,
                                                             bot_session=response))

    response = instance.UpdateBotSession(request, context)

    response.leases[0].state = state.value

    request = bots_pb2.UpdateBotSessionRequest(name=response.name,
                                               bot_session=response)

    response = instance.UpdateBotSession(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.DATA_LOSS)


def test_work_active_to_active(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)
    # Inject work
    action_digest = remote_execution_pb2.Digest(hash='gaff')
    instance._instances[""].execute(action_digest, True)

    # Simulated the severed binding between client and server
    response = copy.deepcopy(instance.CreateBotSession(request, context))

    response.leases[0].state = LeaseState.ACTIVE.value

    request = bots_pb2.UpdateBotSessionRequest(name=response.name,
                                               bot_session=response)

    response = instance.UpdateBotSession(request, context)

    assert response.leases[0].state == LeaseState.ACTIVE.value


def test_post_bot_event_temp(context, instance):
    request = bots_pb2.PostBotEventTempRequest()
    instance.PostBotEventTemp(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)
