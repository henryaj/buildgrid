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

import logging
import uuid

import buildgrid._protos.google.devtools.remoteexecution.v1test.remote_execution_pb2

from enum import Enum

from buildgrid._protos.google.devtools.remoteexecution.v1test.remote_execution_pb2 import ExecuteOperationMetadata, ExecuteResponse
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2, worker_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from google.protobuf import any_pb2

class ExecuteStage(Enum):
    UNKNOWN       = ExecuteOperationMetadata.Stage.Value('UNKNOWN')
    CACHE_CHECK   = ExecuteOperationMetadata.Stage.Value('CACHE_CHECK')
    QUEUED        = ExecuteOperationMetadata.Stage.Value('QUEUED')
    EXECUTING     = ExecuteOperationMetadata.Stage.Value('EXECUTING')
    COMPLETED     = ExecuteOperationMetadata.Stage.Value('COMPLETED')

class BotStatus(Enum):
    BOT_STATUS_UNSPECIFIED = bots_pb2.BotStatus.Value('BOT_STATUS_UNSPECIFIED')
    OK                     = bots_pb2.BotStatus.Value('OK')
    UNHEALTHY              = bots_pb2.BotStatus.Value('UNHEALTHY');
    HOST_REBOOTING         = bots_pb2.BotStatus.Value('HOST_REBOOTING')
    BOT_TERMINATING        = bots_pb2.BotStatus.Value('BOT_TERMINATING')

class LeaseState(Enum):
    LEASE_STATE_UNSPECIFIED = bots_pb2.LeaseState.Value('LEASE_STATE_UNSPECIFIED')
    PENDING                 = bots_pb2.LeaseState.Value('PENDING')
    ACTIVE                  = bots_pb2.LeaseState.Value('ACTIVE')
    COMPLETED               = bots_pb2.LeaseState.Value('COMPLETED')
    CANCELLED               = bots_pb2.LeaseState.Value('CANCELLED')


class Job():

    def __init__(self, action):
        self.action = action
        self.bot_status = BotStatus.BOT_STATUS_UNSPECIFIED
        self.execute_stage = ExecuteStage.UNKNOWN
        self.lease = None
        self.logger = logging.getLogger(__name__)
        self.name = str(uuid.uuid4())
        self.result = None

        self._n_tries = 0
        self._operation = operations_pb2.Operation(name = self.name)

    def get_operation(self):
        self._operation.metadata.CopyFrom(self._pack_any(self.get_operation_meta()))

        if self.result is not None:
            self._operation.done = True
            response = ExecuteResponse()
            self.result.Unpack(response.result)
            self._operation.response.CopyFrom(self._pack_any(response))

        return self._operation

    def get_operation_meta(self):
        meta = ExecuteOperationMetadata()
        meta.stage = self.execute_stage.value

        return meta

    def create_lease(self):
        action = self._pack_any(self.action)

        lease = bots_pb2.Lease(id = self.name,
                               payload = action,
                               state = LeaseState.PENDING.value)
        self.lease = lease
        return lease

    def get_operations(self):
        return operations_pb2.ListOperationsResponse(operations = [self.get_operation()])

    def _pack_any(self, pack):
        any = any_pb2.Any()
        any.Pack(pack)
        return any
