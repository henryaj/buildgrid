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

import pytest

from collections import deque
from unittest import mock

from buildgrid.server import scheduler
from buildgrid.server.job import ExecuteStage, LeaseState

# Instance to test
@pytest.fixture
def instance():
    sched = scheduler.Scheduler()
    sched.jobs = mock.MagicMock(spec = {})
    sched.queue = mock.MagicMock(spec = deque)
    yield sched

def test_append_job(instance):
    mock_job = mock.MagicMock()
    mock_job.return_value.name = ''
    instance.append_job(mock_job)
    instance.jobs.__setitem__.assert_called_once_with(mock_job.name, mock_job)
    instance.queue.append.assert_called_once_with(mock_job)

def test_retry_job(instance):
    n_tries = instance.MAX_N_TRIES - 1
    name = 'eldon'

    mock_job =  mock.MagicMock()
    mock_job.n_tries = n_tries
    instance.jobs.__getitem__.return_value = mock_job

    instance.retry_job(name)

    assert mock_job.execute_stage is ExecuteStage.QUEUED
    assert mock_job.n_tries is n_tries + 1
    instance.jobs.__getitem__.assert_called_once_with(name)
    instance.queue.appendleft.assert_called_once_with(mock_job)
    instance.jobs.__setitem__.assert_called_once_with(name, mock_job)

def test_retry_job_fail(instance):
    n_tries = instance.MAX_N_TRIES
    name = 'eldon'

    mock_job =  mock.MagicMock()
    mock_job.n_tries = n_tries
    instance.jobs.__getitem__.return_value = mock_job

    instance.retry_job(name)

    assert mock_job.execute_stage is ExecuteStage.COMPLETED
    assert mock_job.n_tries is n_tries
    instance.jobs.__setitem__.assert_called_once_with(name, mock_job)

def test_create_job(instance):
    name = 'eldon'

    mock_job =  mock.MagicMock()
    mock_job.name = name
    instance.queue.popleft.return_value = mock_job
    instance.queue.__len__.return_value = 1

    assert instance.create_job() is mock_job

    assert mock_job.execute_stage is ExecuteStage.EXECUTING
    instance.jobs.__setitem__.assert_called_once_with(name, mock_job)

def test_job_complete(instance):
    name = 'eldon'
    result = 'bright'

    mock_job =  mock.MagicMock()
    mock_job.name = name
    instance.jobs.__getitem__.return_value = mock_job

    instance.job_complete(name, result)

    assert mock_job.execute_stage is ExecuteStage.COMPLETED
    assert mock_job.result is result
    instance.jobs.__getitem__.assert_called_once_with(name)
    instance.jobs.__setitem__.assert_called_once_with(name, mock_job)

@pytest.mark.parametrize("state", [LeaseState.LEASE_STATE_UNSPECIFIED,
                                   LeaseState.PENDING,
                                   LeaseState.ACTIVE,
                                   LeaseState.COMPLETED,
                                   mock.Mock(value = 100)
                                   ])
@mock.patch.object(scheduler.Scheduler,'job_complete', autospec = True)
@mock.patch.object(scheduler.Scheduler,'create_job', autospec = True)
def test_update_lease_state_with_work(mock_create_job, mock_job_complete, state, instance):
    name = 'orion'

    mock_lease = mock.Mock()
    mock_lease.assignment = name
    mock_lease.state = state.value

    if state == LeaseState.LEASE_STATE_UNSPECIFIED or \
       state == LeaseState.COMPLETED:
        assert instance.update_lease(mock_lease) is mock_create_job.return_value.lease
        mock_create_job.assert_called_once_with(instance)
        mock_create_job.return_value.create_lease.assert_called_once_with()
        instance.jobs.__setitem__.assert_called_once_with(name, mock_create_job.return_value)

    elif state == LeaseState.PENDING or \
         state == LeaseState.ACTIVE or \
         state == LeaseState.CANCELLED:
        assert instance.update_lease(mock_lease) is mock_lease

    else:
        with pytest.raises(Exception):
            instance.update_lease(mock_lease)

    instance.jobs.get.assert_called_once_with(name)

@pytest.mark.parametrize("state", [LeaseState.LEASE_STATE_UNSPECIFIED,
                                   LeaseState.PENDING,
                                   LeaseState.ACTIVE,
                                   LeaseState.COMPLETED,
                                   mock.Mock(value = 100)
                                   ])
@mock.patch.object(scheduler.Scheduler,'job_complete', autospec = True)
@mock.patch.object(scheduler.Scheduler,'create_job', autospec = True)
def test_update_lease_state_without_work(mock_create_job, mock_job_complete, state, instance):
    name = 'orion'

    mock_lease = mock.Mock()
    mock_lease.assignment = name
    mock_lease.state = state.value

    mock_create_job.return_value = None

    if state == LeaseState.LEASE_STATE_UNSPECIFIED or \
       state == LeaseState.COMPLETED:
        assert instance.update_lease(mock_lease) is mock_lease
        mock_create_job.assert_called_once_with(instance)

    elif state == LeaseState.PENDING or \
         state == LeaseState.ACTIVE or \
         state == LeaseState.CANCELLED:
        assert instance.update_lease(mock_lease) is mock_lease

    else:
        with pytest.raises(Exception):
            instance.update_lease(mock_lease)

    instance.jobs.get.assert_called_once_with(name)

@mock.patch.object(scheduler, 'operations_pb2', autospec = True)
def test_get_operations(mock_pb2, instance):
    value = 'eldon'
    response_value = mock.Mock()
    response_value.get_operation.return_value = value
    response_list = mock.MagicMock(spec = [])
    response_list.return_value = [response_value]
    instance.jobs.configure_mock(values = response_list)

    response = mock.MagicMock()
    mock_pb2.configure_mock(ListOperationsResponse = response)

    assert instance.get_operations() is response.return_value
    response_value.get_operation.assert_called_once()
    response.return_value.operations.extend.assert_called_once_with([value])
    response.assert_called_once_with()
