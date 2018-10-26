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


from enum import Enum

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2


class BotStatus(Enum):
    # Initially unknown state.
    BOT_STATUS_UNSPECIFIED = bots_pb2.BotStatus.Value('BOT_STATUS_UNSPECIFIED')
    # The bot is healthy, and will accept leases as normal.
    OK = bots_pb2.BotStatus.Value('OK')
    # The bot is unhealthy and will not accept new leases.
    UNHEALTHY = bots_pb2.BotStatus.Value('UNHEALTHY')
    # The bot has been asked to reboot the host.
    HOST_REBOOTING = bots_pb2.BotStatus.Value('HOST_REBOOTING')
    # The bot has been asked to shut down.
    BOT_TERMINATING = bots_pb2.BotStatus.Value('BOT_TERMINATING')


class LeaseState(Enum):
    # Initially unknown state.
    LEASE_STATE_UNSPECIFIED = bots_pb2.LeaseState.Value('LEASE_STATE_UNSPECIFIED')
    # The server expects the bot to accept this lease.
    PENDING = bots_pb2.LeaseState.Value('PENDING')
    # The bot has accepted this lease.
    ACTIVE = bots_pb2.LeaseState.Value('ACTIVE')
    # The bot is no longer leased.
    COMPLETED = bots_pb2.LeaseState.Value('COMPLETED')
    # The bot should immediately release all resources associated with the lease.
    CANCELLED = bots_pb2.LeaseState.Value('CANCELLED')


class OperationStage(Enum):
    # Initially unknown stage.
    UNKNOWN = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('UNKNOWN')
    # Checking the result against the cache.
    CACHE_CHECK = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('CACHE_CHECK')
    # Currently idle, awaiting a free machine to execute.
    QUEUED = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('QUEUED')
    # Currently being executed by a worker.
    EXECUTING = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('EXECUTING')
    # Finished execution.
    COMPLETED = remote_execution_pb2.ExecuteOperationMetadata.Stage.Value('COMPLETED')
