# Copyright (C) 2019 Bloomberg LP
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


import copy
import os
import queue
import tempfile
import uuid
from unittest import mock

import grpc
from grpc._server import _Context
import pytest

from buildgrid._enums import LeaseState, OperationStage
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.longrunning import operations_pb2

from buildgrid.utils import create_digest
from buildgrid.server import job
from buildgrid.server.controller import ExecutionController
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.actioncache.instance import ActionCache
from buildgrid.server.execution import service
from buildgrid.server.execution.service import ExecutionService
from buildgrid.server.persistence import DataStore
from buildgrid.server.persistence.mem.impl import MemoryDataStore
from buildgrid.server.persistence.sql.impl import SQLDataStore
from buildgrid.server.persistence.sql import models


server = mock.create_autospec(grpc.server)

command = remote_execution_pb2.Command()
command_digest = create_digest(command.SerializeToString())

action = remote_execution_pb2.Action(command_digest=command_digest,
                                     do_not_cache=True)
action_digest = create_digest(action.SerializeToString())


@pytest.fixture
def context():
    cxt = mock.MagicMock(spec=_Context)
    yield cxt


@pytest.fixture(params=["action-cache", "no-action-cache"])
def controller(request):
    storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)

    write_session = storage.begin_write(command_digest)
    write_session.write(command.SerializeToString())
    storage.commit_write(command_digest, write_session)

    write_session = storage.begin_write(action_digest)
    write_session.write(action.SerializeToString())
    storage.commit_write(action_digest, write_session)

    if request.param == "action-cache":
        cache = ActionCache(storage, 50)
        yield ExecutionController(storage=storage, action_cache=cache)
    else:
        yield ExecutionController(storage=storage)


# Instance to test
@pytest.fixture(params=["mem", "sql"])
def instance(controller, request):
    storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)
    if request.param == "sql":
        _, db = tempfile.mkstemp()
        DataStore.backend = SQLDataStore(storage, connection_string="sqlite:///%s" % db, automigrate=True)
    elif request.param == "mem":
        DataStore.backend = MemoryDataStore(storage)
    try:
        with mock.patch.object(service, 'remote_execution_pb2_grpc'):
            execution_service = ExecutionService(server)
            execution_service.add_instance("", controller.execution_instance)
            yield execution_service
    finally:
        if request.param == "sql":
            DataStore.backend = None
            if os.path.exists(db):
                os.remove(db)


def test_unregister_operation_peer(instance, controller, context):
    scheduler = controller.execution_instance._scheduler
    job_name = scheduler.queue_job_action(action, action_digest, skip_cache_lookup=True)

    message_queue = queue.Queue()
    operation_name = controller.execution_instance.register_job_peer(job_name,
                                                                     context.peer(),
                                                                     message_queue)
    assert operation_name in scheduler._Scheduler__operations_by_peer[context.peer()]

    controller.execution_instance.unregister_operation_peer(operation_name, context.peer())
    job = DataStore.get_job_by_name(job_name)
    assert not scheduler._Scheduler__operations_by_peer[context.peer()]
    assert job is not None

    operation_name = controller.execution_instance.register_job_peer(job_name,
                                                                     context.peer(),
                                                                     message_queue)
    scheduler._update_job_operation_stage(job_name, OperationStage.COMPLETED)
    controller.execution_instance.unregister_operation_peer(operation_name, context.peer())
    if isinstance(DataStore.backend, MemoryDataStore):
        assert DataStore.get_job_by_name(job_name) is None
    elif isinstance(DataStore.backend, SQLDataStore):
        assert job_name not in DataStore.backend.response_cache


@pytest.mark.parametrize("monitoring", [True, False])
def test_update_lease_state(instance, controller, context, monitoring):
    scheduler = controller.execution_instance._scheduler
    if monitoring:
        scheduler.activate_monitoring()

    job_name = scheduler.queue_job_action(action, action_digest, skip_cache_lookup=True)

    job = DataStore.get_job_by_name(job_name)
    job_lease = job.create_lease("test-suite")
    if isinstance(DataStore.backend, SQLDataStore):
        DataStore.create_lease(job_lease)

    lease = copy.deepcopy(job_lease)
    scheduler.update_job_lease_state(job_name, lease)

    lease.state = LeaseState.ACTIVE.value
    scheduler.update_job_lease_state(job_name, lease)
    job = DataStore.get_job_by_name(job_name)
    assert lease.state == job._lease.state

    lease.state = LeaseState.COMPLETED.value
    scheduler.update_job_lease_state(job_name, lease)
    job = DataStore.get_job_by_name(job_name)
    if not isinstance(DataStore.backend, SQLDataStore):
        assert lease.state == job._lease.state
    else:
        assert job._lease is None

    if monitoring:
        # TODO: Actually test that monitoring functioned as expected
        scheduler.deactivate_monitoring()


def test_retry_job_lease(instance, controller, context):
    scheduler = controller.execution_instance._scheduler
    scheduler.MAX_N_TRIES = 2

    job_name = scheduler.queue_job_action(action, action_digest, skip_cache_lookup=True)
    scheduler._update_job_operation_stage(job_name, OperationStage.EXECUTING)

    job = DataStore.get_job_by_name(job_name)

    job_lease = job.create_lease("test-suite")
    if isinstance(DataStore.backend, SQLDataStore):
        DataStore.create_lease(job_lease)

    scheduler.retry_job_lease(job_name)

    job = DataStore.get_job_by_name(job_name)
    assert job.n_tries == 2

    scheduler.retry_job_lease(job_name)

    job = DataStore.get_job_by_name(job_name)
    assert job.n_tries == 2
    assert job.operation_stage == OperationStage.COMPLETED
