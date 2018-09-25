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


from buildgrid._app.settings import parser
from buildgrid._app.commands.cmd_server import _create_server_from_config
from buildgrid.server.cas.service import ByteStreamService, ContentAddressableStorageService
from buildgrid.server.actioncache.service import ActionCacheService
from buildgrid.server.execution.service import ExecutionService
from buildgrid.server.operations.service import OperationsService
from buildgrid.server.bots.service import BotsService
from buildgrid.server.referencestorage.service import ReferenceStorageService

from .utils.cas import run_in_subprocess


config = """
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
        - !disk-storage &main-storage
          path: ~/cas/

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


def test_create_server():
    # Actual test function, to be run in a subprocess:
    def __test_create_server(queue, config_data):
        settings = parser.get_parser().safe_load(config)
        server = _create_server_from_config(settings)

        server.start()
        server.stop()

        try:
            assert isinstance(server._execution_service, ExecutionService)
            assert isinstance(server._operations_service, OperationsService)
            assert isinstance(server._bots_service, BotsService)
            assert isinstance(server._reference_storage_service, ReferenceStorageService)
            assert isinstance(server._action_cache_service, ActionCacheService)
            assert isinstance(server._cas_service, ContentAddressableStorageService)
            assert isinstance(server._bytestream_service, ByteStreamService)
        except AssertionError:
            queue.put(False)
        else:
            queue.put(True)

    assert run_in_subprocess(__test_create_server, config)
