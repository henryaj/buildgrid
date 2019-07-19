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
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid.server.controller import ExecutionController
from buildgrid.server.job import LeaseState, BotStatus
from buildgrid.server.bots import service
from buildgrid.server.bots.service import BotsService

server = mock.create_autospec(grpc.server)


# GRPC context
@pytest.fixture
def context():
    context_mock = mock.MagicMock(spec=_Context)
    context_mock.time_remaining.return_value = 10
    yield context_mock


@pytest.fixture
def bot_session():
    bot = bots_pb2.BotSession()
    bot.bot_id = 'ana'
    bot.status = BotStatus.OK.value
    yield bot


@pytest.fixture
def controller():
    yield ExecutionController()


# Instance to test
@pytest.fixture
def instance(controller):
    instances = {"": controller.bots_interface}
    with mock.patch.object(service, 'bots_pb2_grpc'):
        bots_service = BotsService(server)
        for k, v in instances.items():
            bots_service.add_instance(k, v)
        yield bots_service


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

    for _ in range(0, number_of_jobs):
        _inject_work(instance._instances[""]._scheduler)

    request = bots_pb2.CreateBotSessionRequest(bot_session=bot_session)
    response = instance.CreateBotSession(request, context)

    assert len(response.leases) == min(number_of_jobs, 1)


def test_update_leases_with_work(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)

    action_digest = remote_execution_pb2.Digest(hash='gaff')
    _inject_work(instance._instances[""]._scheduler, action_digest=action_digest)

    response = instance.CreateBotSession(request, context)

    assert len(response.leases) == 1
    response_action = remote_execution_pb2.Digest()
    response.leases[0].payload.Unpack(response_action)

    assert isinstance(response, bots_pb2.BotSession)
    assert response.leases[0].state == LeaseState.PENDING.value
    assert response_action == action_digest


def test_post_bot_event_temp(context, instance):
    request = bots_pb2.PostBotEventTempRequest()
    instance.PostBotEventTemp(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)


def test_unmet_platform_requirements(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)

    action_digest = remote_execution_pb2.Digest(hash='gaff')
    _inject_work(instance._instances[""]._scheduler,
                 action_digest=action_digest,
                 platform_requirements={'OSFamily': set('wonderful-os')})

    response = instance.CreateBotSession(request, context)

    assert len(response.leases) == 0


def test_unhealthy_bot(bot_session, context, instance):
    # set botstatus to unhealthy
    bot_session.status = BotStatus.UNHEALTHY.value
    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot_session)

    action_digest = remote_execution_pb2.Digest(hash='gaff')
    _inject_work(instance._instances[""]._scheduler, action_digest=action_digest)

    response = instance.CreateBotSession(request, context)

    # No leases should be given
    assert len(response.leases) == 0


def _inject_work(scheduler, action=None, action_digest=None,
                 platform_requirements=None):
    if not action:
        action = remote_execution_pb2.Action()

    if not action_digest:
        action_digest = remote_execution_pb2.Digest()

    scheduler.queue_job_action(action, action_digest, platform_requirements,
                               skip_cache_lookup=True)
