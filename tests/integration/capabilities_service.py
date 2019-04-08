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


import grpc
import pytest

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.client.capabilities import CapabilitiesInterface
from buildgrid.server.controller import ExecutionController
from buildgrid.server.actioncache.storage import ActionCache
from buildgrid.server.cas.instance import ContentAddressableStorageInstance
from buildgrid.server.cas.storage.lru_memory_cache import LRUMemoryCache

from ..utils.utils import run_in_subprocess
from ..utils.capabilities import serve_capabilities_service


INSTANCES = ['', 'instance']


# Use subprocess to avoid creation of gRPC threads in main process
# See https://github.com/grpc/grpc/blob/master/doc/fork_support.md
# Multiprocessing uses pickle which protobufs don't work with
# Workaround wrapper to send messages as strings
class ServerInterface:

    def __init__(self, remote):
        self.__remote = remote

    def get_capabilities(self, instance_name):

        def __get_capabilities(queue, remote, instance_name):
            interface = CapabilitiesInterface(grpc.insecure_channel(remote))

            result = interface.get_capabilities(instance_name)
            queue.put(result.SerializeToString())

        result = run_in_subprocess(__get_capabilities,
                                   self.__remote, instance_name)

        capabilities = remote_execution_pb2.ServerCapabilities()
        capabilities.ParseFromString(result)
        return capabilities


@pytest.mark.parametrize('instance', INSTANCES)
def test_execution_not_available_capabilities(instance):
    with serve_capabilities_service([instance]) as server:
        server_interface = ServerInterface(server.remote)
        response = server_interface.get_capabilities(instance)

        assert not response.execution_capabilities.exec_enabled


@pytest.mark.parametrize('instance', INSTANCES)
def test_execution_available_capabilities(instance):
    controller = ExecutionController()

    with serve_capabilities_service([instance],
                                    execution_instance=controller.execution_instance) as server:
        server_interface = ServerInterface(server.remote)
        response = server_interface.get_capabilities(instance)

        assert response.execution_capabilities.exec_enabled
        assert response.execution_capabilities.digest_function


@pytest.mark.parametrize('instance', INSTANCES)
def test_action_cache_allow_updates_capabilities(instance):
    storage = LRUMemoryCache(limit=256)
    action_cache = ActionCache(storage, max_cached_refs=256, allow_updates=True)

    with serve_capabilities_service([instance],
                                    action_cache_instance=action_cache) as server:
        server_interface = ServerInterface(server.remote)
        response = server_interface.get_capabilities(instance)

        assert response.cache_capabilities.action_cache_update_capabilities.update_enabled


@pytest.mark.parametrize('instance', INSTANCES)
def test_action_cache_not_allow_updates_capabilities(instance):
    storage = LRUMemoryCache(limit=256)
    action_cache = ActionCache(storage, max_cached_refs=256, allow_updates=False)

    with serve_capabilities_service([instance],
                                    action_cache_instance=action_cache) as server:
        server_interface = ServerInterface(server.remote)
        response = server_interface.get_capabilities(instance)

        assert not response.cache_capabilities.action_cache_update_capabilities.update_enabled
        assert len(response.cache_capabilities.digest_function) == 1
        assert response.cache_capabilities.digest_function[0]


@pytest.mark.parametrize('instance', INSTANCES)
def test_cas_capabilities(instance):
    cas = ContentAddressableStorageInstance(None)

    with serve_capabilities_service([instance],
                                    cas_instance=cas) as server:
        server_interface = ServerInterface(server.remote)
        response = server_interface.get_capabilities(instance)

        assert len(response.cache_capabilities.digest_function) == 1
        assert response.cache_capabilities.digest_function[0]
        assert response.cache_capabilities.symlink_absolute_path_strategy
        assert response.cache_capabilities.max_batch_total_size_bytes
