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

# pylint: disable=redefined-outer-name

from unittest import mock

import grpc
from grpc._server import _Context

import pytest

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid._protos.buildstream.v2 import buildstream_pb2

from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.referencestorage import service
from buildgrid.server.referencestorage.service import ReferenceStorageService
from buildgrid.server.referencestorage.storage import ReferenceCache


server = mock.create_autospec(grpc.server)
instance_name = ''


# Can mock this
@pytest.fixture
def context():
    yield mock.MagicMock(spec=_Context)


@pytest.fixture
def cas():
    yield lru_memory_cache.LRUMemoryCache(1024 * 1024)


@pytest.fixture
def cache(cas):
    yield ReferenceCache(cas, 50)


@pytest.fixture
def instance(cache):
    with mock.patch.object(service, 'buildstream_pb2_grpc'):
        ref_service = ReferenceStorageService(server)
        ref_service.add_instance(instance_name, cache)
        yield ref_service


def test_simple_result(instance, context):
    keys = ["rick", "roy", "rach"]

    # Check that before adding the ReferenceResult, attempting to fetch it fails
    request = buildstream_pb2.GetReferenceRequest(key=keys[0])
    instance.GetReference(request, context)
    context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)

    # Add an ReferenceResult to the cache
    reference_result = remote_execution_pb2.Digest(hash='deckard')
    request = buildstream_pb2.UpdateReferenceRequest(keys=keys,
                                                     digest=reference_result)
    instance.UpdateReference(request, context)

    # Check that fetching it now works
    for key in keys:
        request = buildstream_pb2.GetReferenceRequest(key=key)
        fetched_result = instance.GetReference(request, context)
        assert fetched_result.digest == reference_result


def test_disabled_update_result(context):
    disabled_push = ReferenceCache(cas, 50, False)
    keys = ["rick", "roy", "rach"]

    with mock.patch.object(service, 'buildstream_pb2_grpc'):
        instance = ReferenceStorageService(server)
        instance.add_instance(instance_name, disabled_push)

    # Add an ReferenceResult to the cache
    reference_result = remote_execution_pb2.Digest(hash='deckard')
    request = buildstream_pb2.UpdateReferenceRequest(keys=keys,
                                                     digest=reference_result)
    instance.UpdateReference(request, context)

    request = buildstream_pb2.UpdateReferenceRequest()
    instance.UpdateReference(request, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.UNIMPLEMENTED)


@pytest.mark.parametrize("allow_updates", [True, False])
def test_status(allow_updates, context):
    cache = ReferenceCache(cas, 5, allow_updates)
    with mock.patch.object(service, 'buildstream_pb2_grpc'):
        instance = ReferenceStorageService(server)
        instance.add_instance("", cache)

    request = buildstream_pb2.StatusRequest()
    response = instance.Status(request, context)

    assert response.allow_updates == allow_updates
