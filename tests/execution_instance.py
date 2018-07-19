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

from unittest import mock

from buildgrid.server import scheduler, job
from buildgrid.server.execution import execution_instance
from buildgrid.server.execution._exceptions import InvalidArgumentError

# Mock of the scheduler class
@pytest.fixture
def schedule():
    sched = mock.MagicMock(spec = scheduler.Scheduler)
    sched.configure_mock(jobs = mock.MagicMock(spec = {}))
    yield sched

# Actual instance to test
@pytest.fixture
def instance(schedule):
    yield execution_instance.ExecutionInstance(schedule)

@pytest.mark.parametrize("skip_cache_lookup", [True, False])
@mock.patch.object(execution_instance,'Job', autospec = True)
def test_execute(mock_job, skip_cache_lookup, instance):
    return_operation = 'rick'
    action = 'pris'
    mock_job.return_value.name = ''
    mock_job.return_value.get_operation.return_value = return_operation

    if skip_cache_lookup is False:
        with pytest.raises(NotImplementedError):
            instance.execute(action=action,
                             skip_cache_lookup=skip_cache_lookup)
    else:
        assert instance.execute(action=action,
                                skip_cache_lookup=skip_cache_lookup) is return_operation
        # Action must be stored in the job
        mock_job.assert_called_once_with(action)
        # Only create one job per execute
        instance._scheduler.append_job.assert_called_once_with(mock_job.return_value)

def test_get_operation(instance):
    return_operation = mock.MagicMock(spec = job.Job)
    instance._scheduler.jobs.get.return_value = return_operation

    assert instance.get_operation('') is return_operation.get_operation.return_value

def test_get_operation_fail(instance):
    instance._scheduler.jobs.get.return_value = None

    with pytest.raises(InvalidArgumentError):
        instance.get_operation('')

def test_list_operations(instance):
    # List interface hasn't been fully implemented yet
    instance.list_operations('change', 'me', 'in', 'future')
    assert instance._scheduler.get_operations.call_count is 1

def test_delete_operation(instance):
    name = 'roy'
    instance._scheduler.jobs = mock.MagicMock(spec = {})
    instance.delete_operation(name)
    instance._scheduler.jobs.pop.assert_called_once_with(name)

def test_delete_operation_fail(instance):
    name = 'roy'
    instance._scheduler.jobs.pop.side_effect = KeyError()
    with pytest.raises(InvalidArgumentError):
        instance.delete_operation(name)

def test_cancel_operation(instance):
    with pytest.raises(NotImplementedError):
        instance.cancel_operation('')
