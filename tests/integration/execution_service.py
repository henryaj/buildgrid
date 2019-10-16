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
import queue
import tempfile
import uuid
from unittest import mock

import grpc
from grpc._server import _Context
import pytest

from buildgrid._enums import OperationStage
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.longrunning import operations_pb2

from buildgrid.utils import create_digest
from buildgrid.server import job
from buildgrid.server.controller import ExecutionController
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.actioncache.instance import ActionCache
from buildgrid.server.execution import service
from buildgrid.server.execution.service import ExecutionService
from buildgrid.server.persistence.mem.impl import MemoryDataStore
from buildgrid.server.persistence.sql.impl import SQLDataStore
from buildgrid.server.persistence.sql import models


server = mock.create_autospec(grpc.server)

command = remote_execution_pb2.Command()
command.platform.properties.add(name="OSFamily", value="linux")
command.platform.properties.add(name="ISA", value="x86")
command_digest = create_digest(command.SerializeToString())

action = remote_execution_pb2.Action(command_digest=command_digest,
                                     do_not_cache=True)
action_digest = create_digest(action.SerializeToString())


@pytest.fixture
def context():
    cxt = mock.MagicMock(spec=_Context)
    yield cxt


PARAMS = [(impl, use_cache) for impl in ["sql", "mem"]
          for use_cache in ["action-cache", "no-action-cache"]]


@pytest.fixture(params=PARAMS)
def controller(request):
    impl, use_cache = request.param
    storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)

    write_session = storage.begin_write(command_digest)
    write_session.write(command.SerializeToString())
    storage.commit_write(command_digest, write_session)

    write_session = storage.begin_write(action_digest)
    write_session.write(action.SerializeToString())
    storage.commit_write(action_digest, write_session)

    if impl == "sql":
        _, db = tempfile.mkstemp()
        data_store = SQLDataStore(storage, connection_string="sqlite:///%s" % db, automigrate=True)
    elif impl == "mem":
        data_store = MemoryDataStore(storage)
    try:
        if use_cache == "action-cache":
            cache = ActionCache(storage, 50)
            yield ExecutionController(data_store, storage=storage, action_cache=cache)
        else:
            yield ExecutionController(data_store, storage=storage)
    finally:
        if impl == "sql":
            if os.path.exists(db):
                os.remove(db)


# Instance to test
@pytest.fixture(params=["mem", "sql"])
def instance(controller, request):
    with mock.patch.object(service, 'remote_execution_pb2_grpc'):
        execution_service = ExecutionService(server)
        execution_service.add_instance("", controller.execution_instance)
        yield execution_service


@pytest.mark.parametrize("skip_cache_lookup", [True, False])
def test_execute(skip_cache_lookup, instance, context):
    request = remote_execution_pb2.ExecuteRequest(instance_name='',
                                                  action_digest=action_digest,
                                                  skip_cache_lookup=skip_cache_lookup)
    response = instance.Execute(request, context)

    result = next(response)
    assert isinstance(result, operations_pb2.Operation)
    metadata = remote_execution_pb2.ExecuteOperationMetadata()
    result.metadata.Unpack(metadata)
    assert metadata.stage == OperationStage.QUEUED.value
    operation_uuid = result.name.split('/')[-1]
    assert uuid.UUID(operation_uuid, version=4)
    assert result.done is False


def test_wrong_execute_instance(instance, context):
    request = remote_execution_pb2.ExecuteRequest(instance_name='blade')
    response = instance.Execute(request, context)

    next(response)
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_no_action_digest_in_storage(instance, context):
    request = remote_execution_pb2.ExecuteRequest(instance_name='',
                                                  skip_cache_lookup=True)
    response = instance.Execute(request, context)

    next(response)
    context.set_code.assert_called_once_with(grpc.StatusCode.FAILED_PRECONDITION)


def test_wait_execution(instance, controller, context):
    scheduler = controller.execution_instance._scheduler

    job_name = scheduler.queue_job_action(action, action_digest, skip_cache_lookup=True)

    message_queue = queue.Queue()
    operation_name = controller.execution_instance.register_job_peer(job_name,
                                                                     context.peer(),
                                                                     message_queue)

    scheduler._update_job_operation_stage(job_name, OperationStage.COMPLETED)

    request = remote_execution_pb2.WaitExecutionRequest(name=operation_name)

    response = instance.WaitExecution(request, context)

    result = next(response)

    assert isinstance(result, operations_pb2.Operation)
    metadata = remote_execution_pb2.ExecuteOperationMetadata()
    result.metadata.Unpack(metadata)
    assert metadata.stage == job.OperationStage.COMPLETED.value
    assert result.done is True

    if isinstance(scheduler.data_store, SQLDataStore):
        with scheduler.data_store.session() as session:
            record = session.query(models.Job).filter_by(name=job_name).first()
            assert record is not None
            assert record.stage == job.OperationStage.COMPLETED.value
            assert record.operations
            assert all(op.done for op in record.operations)


def test_wrong_instance_wait_execution(instance, context):
    request = remote_execution_pb2.WaitExecutionRequest(name="blade")
    next(instance.WaitExecution(request, context))

    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)


def test_job_deduplication_in_scheduling(instance, controller, context):
    scheduler = controller.execution_instance._scheduler

    action = remote_execution_pb2.Action(command_digest=command_digest,
                                         do_not_cache=False)
    action_digest = create_digest(action.SerializeToString())

    job_name1 = scheduler.queue_job_action(action, action_digest, skip_cache_lookup=True)

    message_queue = queue.Queue()
    operation_name1 = controller.execution_instance.register_job_peer(job_name1,
                                                                      context.peer(),
                                                                      message_queue)

    job_name2 = scheduler.queue_job_action(action, action_digest, skip_cache_lookup=True)

    operation_name2 = controller.execution_instance.register_job_peer(job_name2,
                                                                      context.peer(),
                                                                      message_queue)
    # The jobs are be deduplicated, but and operations are created
    assert job_name1 == job_name2
    assert operation_name1 != operation_name2

    if isinstance(scheduler.data_store, SQLDataStore):
        with scheduler.data_store.session() as session:
            query = session.query(models.Job)
            job_count = query.filter_by(name=job_name1).count()
            assert job_count == 1
            query = session.query(models.Operation)
            operation_count = query.filter_by(job_name=job_name1).count()
            assert operation_count == 2


def test_job_reprioritisation(instance, controller, context):
    scheduler = controller.execution_instance._scheduler

    action = remote_execution_pb2.Action(command_digest=command_digest)
    action_digest = create_digest(action.SerializeToString())

    job_name1 = scheduler.queue_job_action(
        action, action_digest, skip_cache_lookup=True, priority=10)

    job = scheduler.data_store.get_job_by_name(job_name1)
    assert job.priority == 10

    job_name2 = scheduler.queue_job_action(
        action, action_digest, skip_cache_lookup=True, priority=1)

    assert job_name1 == job_name2
    job = scheduler.data_store.get_job_by_name(job_name1)
    assert job.priority == 1


@pytest.mark.parametrize("do_not_cache", [True, False])
def test_do_not_cache_no_deduplication(do_not_cache, instance, controller, context):
    scheduler = controller.execution_instance._scheduler

    # The default action already has do_not_cache set, so use that
    job_name1 = scheduler.queue_job_action(action, action_digest, skip_cache_lookup=True)

    message_queue = queue.Queue()
    operation_name1 = controller.execution_instance.register_job_peer(job_name1,
                                                                      context.peer(),
                                                                      message_queue)

    action2 = remote_execution_pb2.Action(command_digest=command_digest,
                                          do_not_cache=do_not_cache)

    action_digest2 = create_digest(action2.SerializeToString())
    job_name2 = scheduler.queue_job_action(action2, action_digest2, skip_cache_lookup=True)

    operation_name2 = controller.execution_instance.register_job_peer(job_name2,
                                                                      context.peer(),
                                                                      message_queue)
    # The jobs are not be deduplicated because of do_not_cache,
    # and two operations are created
    assert job_name1 != job_name2
    assert operation_name1 != operation_name2

    if isinstance(scheduler.data_store, SQLDataStore):
        with scheduler.data_store.session() as session:
            job_count = session.query(models.Job).count()
            assert job_count == 2

            operation_count = session.query(models.Operation).count()
            assert operation_count == 2
