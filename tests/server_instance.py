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


import time

import grpc
import pytest

from buildgrid._enums import BotStatus
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2_grpc
from buildgrid._protos.buildstream.v2 import buildstream_pb2
from buildgrid._protos.buildstream.v2 import buildstream_pb2_grpc
from buildgrid._protos.google.bytestream import bytestream_pb2
from buildgrid._protos.google.bytestream import bytestream_pb2_grpc
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2_grpc
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid._protos.google.longrunning import operations_pb2_grpc

from .utils.utils import run_in_subprocess
from .utils.server import serve


CONFIGURATION = """
server:
  - !channel
    port: 50051
    insecure_mode: true
    credentials:
      tls-server-key: null
      tls-server-cert: null
      tls-client-certs: null

description: |
  A single default instance

instances:
  - name: main
    description: |
      The main server

    storages:
        - !lru-storage &main-storage
          size: 256mb

    services:
      - !action-cache &main-action
        storage: *main-storage
        max_cached_refs: 256
        allow_updates: true

      - !execution
        storage: *main-storage
        action_cache: *main-action

      - !cas
        storage: *main-storage

      - !bytestream
        storage: *main-storage

      - !reference-cache
        storage: *main-storage
        max_cached_refs: 256
        allow_updates: true
"""


@pytest.mark.parametrize("monitoring", [True, False])
def test_create_server(monitoring):
    # Actual test function, to be run in a subprocess:
    def __test_create_server(queue, remote):
        # Open a channel to the remote server:
        channel = grpc.insecure_channel(remote)

        try:
            stub = remote_execution_pb2_grpc.ExecutionStub(channel)
            request = remote_execution_pb2.ExecuteRequest(instance_name='main')
            response = next(stub.Execute(request))

            assert response.DESCRIPTOR is operations_pb2.Operation.DESCRIPTOR

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                queue.put(False)
        except AssertionError:
            queue.put(False)

        try:
            stub = remote_execution_pb2_grpc.ActionCacheStub(channel)
            request = remote_execution_pb2.GetActionResultRequest(instance_name='main')
            response = stub.GetActionResult(request)

            assert response.DESCRIPTOR is remote_execution_pb2.ActionResult.DESCRIPTOR

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                queue.put(False)
        except AssertionError:
            queue.put(False)

        try:
            stub = remote_execution_pb2_grpc.ContentAddressableStorageStub(channel)
            request = remote_execution_pb2.BatchUpdateBlobsRequest(instance_name='main')
            response = stub.BatchUpdateBlobs(request)

            assert response.DESCRIPTOR is remote_execution_pb2.BatchUpdateBlobsResponse.DESCRIPTOR

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                queue.put(False)
        except AssertionError:
            queue.put(False)

        try:
            stub = buildstream_pb2_grpc.ReferenceStorageStub(channel)
            request = buildstream_pb2.GetReferenceRequest(instance_name='main')
            response = stub.GetReference(request)

            assert response.DESCRIPTOR is buildstream_pb2.GetReferenceResponse.DESCRIPTOR

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                queue.put(False)
        except AssertionError:
            queue.put(False)

        try:
            stub = bytestream_pb2_grpc.ByteStreamStub(channel)
            request = bytestream_pb2.ReadRequest()
            response = stub.Read(request)

            assert next(response).DESCRIPTOR is bytestream_pb2.ReadResponse.DESCRIPTOR

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                queue.put(False)
        except AssertionError:
            queue.put(False)

        try:
            stub = operations_pb2_grpc.OperationsStub(channel)
            request = operations_pb2.ListOperationsRequest(name='main')
            response = stub.ListOperations(request)

            assert response.DESCRIPTOR is operations_pb2.ListOperationsResponse.DESCRIPTOR

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                queue.put(False)
        except AssertionError:
            queue.put(False)

        try:
            session = bots_pb2.BotSession(leases=[],
                                          bot_id="test-bot",
                                          name="test-bot",
                                          status=BotStatus.OK.value)
            stub = bots_pb2_grpc.BotsStub(channel)
            request = bots_pb2.CreateBotSessionRequest(parent='main', bot_session=session)
            response = stub.CreateBotSession(request)

            assert response.DESCRIPTOR is bots_pb2.BotSession.DESCRIPTOR

        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                queue.put(False)
        except AssertionError:
            queue.put(False)

        # Sleep for 5 seconds to allow a useful number of metrics to get reported
        time.sleep(5)
        queue.put(True)

    with serve(CONFIGURATION, monitoring) as (server, path):
        assert run_in_subprocess(__test_create_server, server.remote, timeout=30)

        if monitoring:
            with open(path) as f:
                metrics = [line.strip() for line in f.readlines()]
            # There should be a bots-count record from before we connected the bot,
            # and also one from after the bot had made its initial connection
            assert "main.instance.bots-count:0|g" in metrics
            assert "main.instance.bots-count:1|g" in metrics
