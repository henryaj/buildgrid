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


import os
from datetime import datetime, timedelta
import tempfile
import time
from unittest import mock

import asyncio
import grpc
from grpc._server import _Context
import pytest

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.controller import ExecutionController
from buildgrid.server.job import LeaseState, BotStatus
from buildgrid.server.bots import service
from buildgrid.server.bots.service import BotsService
from buildgrid.server.bots.instance import BotsInterface
from buildgrid.server.persistence.mem.impl import MemoryDataStore
from buildgrid.server.persistence.sql.impl import SQLDataStore

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


BOT_SESSION_KEEPALIVE_TIMEOUT_OPTIONS = [None, 1, 2]
DATA_STORE_IMPLS = ["sql", "mem"]

PARAMS = [(impl, timeout)
          for timeout in BOT_SESSION_KEEPALIVE_TIMEOUT_OPTIONS
          for impl in DATA_STORE_IMPLS]


@pytest.fixture(params=PARAMS)
def controller(request):
    storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)
    impl, timeout = request.param
    if impl == "sql":
        _, db = tempfile.mkstemp()
        data_store = SQLDataStore(storage, connection_string="sqlite:///%s" % db, automigrate=True)
    elif impl == "mem":
        data_store = MemoryDataStore(storage)
    try:
        yield ExecutionController(data_store, bot_session_keepalive_timeout=timeout)
    finally:
        if impl == "sql":
            if os.path.exists(db):
                os.remove(db)


# Instance to test
@pytest.fixture
def instance(controller, request):
    instances = {"": controller.bots_interface}
    with mock.patch.object(service, 'bots_pb2_grpc'):
        bots_service = BotsService(server)
        for k, v in instances.items():
            bots_service.add_instance(k, v)
        yield bots_service


def check_bot_session_request_response_and_assigned_expiry(instance, request, response, request_time):
    assert isinstance(response, bots_pb2.BotSession)

    bot_session_keepalive_timeout = instance._instances[""]._bot_session_keepalive_timeout

    assert request.bot_id == response.bot_id

    if bot_session_keepalive_timeout:
        # See if expiry time was set (when enabled)
        assert response.expire_time.IsInitialized()
        # See if it was set to an expected time
        earliest_expire_time = request_time + timedelta(seconds=bot_session_keepalive_timeout)
        # We don't want to do strict comparisons because we are not looking for
        # exact time precision (e.g. in ms/ns)
        response_expire_time = response.expire_time.ToDatetime()
        assert response_expire_time >= earliest_expire_time


def test_create_bot_session(bot_session, context, instance):
    request_time = datetime.utcnow()
    request = bots_pb2.CreateBotSessionRequest(bot_session=bot_session)

    response = instance.CreateBotSession(request, context)

    check_bot_session_request_response_and_assigned_expiry(instance, bot_session, response, request_time)


def test_create_bot_session_bot_id_fail(context, instance):
    bot = bots_pb2.BotSession()

    request = bots_pb2.CreateBotSessionRequest(parent='',
                                               bot_session=bot)

    instance.CreateBotSession(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_update_bot_session(bot_session, context, instance):
    request = bots_pb2.CreateBotSessionRequest(bot_session=bot_session)
    bot = instance.CreateBotSession(request, context)

    request_time = datetime.utcnow()
    request = bots_pb2.UpdateBotSessionRequest(name=bot.name,
                                               bot_session=bot)

    response = instance.UpdateBotSession(request, context)

    check_bot_session_request_response_and_assigned_expiry(instance, bot_session, response, request_time)


@pytest.mark.parametrize("sleep_duration", [1, 2])
def test_update_bot_session_and_wait_for_expiry(bot_session, context, instance, sleep_duration):
    bots_interface = instance._get_instance("")
    bot_session_keepalive_timeout = bots_interface._bot_session_keepalive_timeout

    with mock.patch.object(bots_interface, '_close_bot_session', autospec=True) as close_botsession_fn:
        request = bots_pb2.CreateBotSessionRequest(bot_session=bot_session)
        bot = instance.CreateBotSession(request, context)

        request_time = datetime.utcnow()
        request = bots_pb2.UpdateBotSessionRequest(name=bot.name,
                                                   bot_session=bot)

        response = instance.UpdateBotSession(request, context)

        check_bot_session_request_response_and_assigned_expiry(instance, bot_session, response, request_time)

        if bot_session_keepalive_timeout:
            assert bots_interface._next_expire_time_occurs_in() >= 0

        time.sleep(sleep_duration)
        bots_interface._reap_next_expired_session()

        # Call this manually since the asyncio event loop isn't running
        if bot_session_keepalive_timeout and bot_session_keepalive_timeout <= sleep_duration:
            # the BotSession should have expired after sleeping `sleep_duration`
            assert close_botsession_fn.call_count == 1
        else:
            # no timeout, or timeout > 1, shouldn't see any expiries yet.
            assert close_botsession_fn.call_count == 0


@pytest.mark.parametrize("bot_session_keepalive_timeout", BOT_SESSION_KEEPALIVE_TIMEOUT_OPTIONS)
def test_bots_instance_sets_up_reaper_loop(bot_session_keepalive_timeout):
    scheduler = mock.Mock()
    main_loop = asyncio.get_event_loop()
    with mock.patch.object(main_loop, 'create_task', autospec=True) as loop_create_task_fn:
        with mock.patch.object(BotsInterface, '_reap_expired_sessions_loop', autospec=True):
            # Just instantiate and see whether the __init__ sets up the reaper loop when needed
            my_instance = BotsInterface(scheduler, bot_session_keepalive_timeout=bot_session_keepalive_timeout)

            # If the timeout was set, the reaper task should have been created, otherwise it shouldn't
            if bot_session_keepalive_timeout:
                assert loop_create_task_fn.call_count == 1
            else:
                assert loop_create_task_fn.call_count == 0


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
