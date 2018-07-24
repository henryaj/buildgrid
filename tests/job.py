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

from buildgrid.server import job
from buildgrid.server.job import ExecuteStage, LeaseState

from google.devtools.remoteexecution.v1test import remote_execution_pb2
from google.longrunning import operations_pb2

# Instance to test
@pytest.fixture
def instance():
    j = job.Job('skin')
    j._operation = mock.MagicMock(spec = operations_pb2.Operation)
    j._operation.response = mock.Mock()
    j._operation.metadata = mock.MagicMock(spec = remote_execution_pb2.ExecuteOperationMetadata)
    yield j

@pytest.mark.parametrize("execute_stage", [ExecuteStage.QUEUED, ExecuteStage.COMPLETED])
@mock.patch.object(job.Job,'_pack_any', autospec = True)
@mock.patch.object(job.Job,'get_operation_meta', autospec = True)
def test_get_opertation(mock_get_meta, mock_pack, execute_stage, instance):
    instance.execute_stage = execute_stage
    result = 'orion'
    instance.result = result

    assert instance.get_operation() is instance._operation

    instance._operation.metadata.CopyFrom.assert_called_once_with(mock_pack.return_value)
    mock_get_meta.assert_called_once_with(instance)

    if execute_stage is ExecuteStage.COMPLETED:
        assert instance._operation.done
        instance._operation.response.CopyFrom.assert_called_once_with(mock_pack.return_value)
        mock_pack.assert_called_with(instance, result)
        assert mock_pack.call_count is 2
    else:
        mock_pack.assert_called_once_with(instance, mock_get_meta.return_value)

@mock.patch.object(job,'ExecuteOperationMetadata', autospec = True)
def test_get_operation_meta(mock_meta, instance):
    instance.execute_stage = ExecuteStage.COMPLETED
    response = instance.get_operation_meta()

    assert response is mock_meta.return_value
    assert response.stage is instance.execute_stage.value

@mock.patch.object(job.bots_pb2, 'Lease', autospec = True)
@mock.patch.object(job.Job,'_pack_any', autospec = True)
def test_create_lease(mock_pack, mock_lease, instance):
    action='harry'
    name = 'bryant'
    lease_state = LeaseState.PENDING

    instance.action = action
    instance.name = name
    instance.lease_state = lease_state

    assert instance.create_lease() is mock_lease.return_value
    mock_pack.assert_called_once_with(instance, action)
    mock_lease.assert_called_once_with(assignment=name,
                                       inline_assignment=mock_pack.return_value,
                                       state=lease_state.value)

@mock.patch.object(job.Job, 'get_operation', autospec = True)
@mock.patch.object(job.operations_pb2,'ListOperationsResponse', autospec = True)
def test_get_operations(mock_response, mock_get_operation, instance):
    assert instance.get_operations() is mock_response.return_value
    mock_get_operation.assert_called_once_with(instance)
    mock_response.assert_called_once_with(operations=[mock_get_operation.return_value])

@mock.patch.object(job.any_pb2, 'Any', autospec = True)
def test__pack_any(mock_any, instance):
    pack = 'rach'
    assert instance._pack_any(pack) is mock_any.return_value

    mock_any.assert_called_once_with()
