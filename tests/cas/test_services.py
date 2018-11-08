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
# Authors:
#        Carter Sande <csande@bloomberg.net>

# pylint: disable=redefined-outer-name

import io
from unittest import mock

import grpc
from grpc._server import _Context
import pytest

from buildgrid._protos.google.bytestream import bytestream_pb2
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2 as re_pb2
from buildgrid.server.cas.storage.storage_abc import StorageABC
from buildgrid.server.cas.instance import ByteStreamInstance, ContentAddressableStorageInstance
from buildgrid.server.cas import service
from buildgrid.server.cas.service import ByteStreamService, ContentAddressableStorageService
from buildgrid.settings import HASH


context = mock.create_autospec(_Context)
server = mock.create_autospec(grpc.server)


class SimpleStorage(StorageABC):
    """Storage provider wrapper around a dictionary.

    Does not attempt to delete old entries, so this is only useful for testing.
    """

    def __init__(self, existing_data=None):
        self.data = {}
        if existing_data:
            for datum in existing_data:
                self.data[(HASH(datum).hexdigest(), len(datum))] = datum

    def has_blob(self, digest):
        return (digest.hash, digest.size_bytes) in self.data

    def get_blob(self, digest):
        key = (digest.hash, digest.size_bytes)
        return io.BytesIO(self.data[key]) if key in self.data else None

    def begin_write(self, digest):
        result = io.BytesIO()
        result.digest = digest
        return result

    def commit_write(self, digest, write_session):
        assert write_session.digest == digest
        data = write_session.getvalue()
        assert HASH(data).hexdigest() == digest.hash
        assert len(data) == digest.size_bytes
        self.data[(digest.hash, digest.size_bytes)] = data


test_strings = [b"", b"hij"]
instances = ["", "test_inst"]


@pytest.mark.parametrize("data_to_read", test_strings)
@pytest.mark.parametrize("instance", instances)
@mock.patch.object(service, 'bytestream_pb2_grpc', autospec=True)
def test_bytestream_read(mocked, data_to_read, instance):
    storage = SimpleStorage([b"abc", b"defg", data_to_read])

    bs_instance = ByteStreamInstance(storage)
    servicer = ByteStreamService(server)
    servicer.add_instance(instance, bs_instance)

    request = bytestream_pb2.ReadRequest()
    if instance != "":
        request.resource_name = instance + "/"
    request.resource_name += "blobs/{}/{}".format(HASH(data_to_read).hexdigest(), len(data_to_read))

    data = b""
    for response in servicer.Read(request, context):
        data += response.data
    assert data == data_to_read


@pytest.mark.parametrize("instance", instances)
@mock.patch.object(service, 'bytestream_pb2_grpc', autospec=True)
def test_bytestream_read_many(mocked, instance):
    data_to_read = b"testing" * 10000

    storage = SimpleStorage([b"abc", b"defg", data_to_read])

    bs_instance = ByteStreamInstance(storage)
    servicer = ByteStreamService(server)
    servicer.add_instance(instance, bs_instance)

    request = bytestream_pb2.ReadRequest()
    if instance != "":
        request.resource_name = instance + "/"
    request.resource_name += "blobs/{}/{}".format(HASH(data_to_read).hexdigest(), len(data_to_read))

    data = b""
    for response in servicer.Read(request, context):
        data += response.data
    assert data == data_to_read


@pytest.mark.parametrize("instance", instances)
@pytest.mark.parametrize("extra_data", ["", "/", "/extra/data"])
@mock.patch.object(service, 'bytestream_pb2_grpc', autospec=True)
def test_bytestream_write(mocked, instance, extra_data):
    storage = SimpleStorage()

    bs_instance = ByteStreamInstance(storage)
    servicer = ByteStreamService(server)
    servicer.add_instance(instance, bs_instance)

    resource_name = ""
    if instance != "":
        resource_name = instance + "/"
    hash_ = HASH(b'abcdef').hexdigest()
    resource_name += "uploads/UUID-HERE/blobs/{}/6".format(hash_)
    resource_name += extra_data
    requests = [
        bytestream_pb2.WriteRequest(resource_name=resource_name, data=b'abc'),
        bytestream_pb2.WriteRequest(data=b'def', write_offset=3, finish_write=True)
    ]

    response = servicer.Write(requests, context)
    assert response.committed_size == 6
    assert len(storage.data) == 1
    assert (hash_, 6) in storage.data
    assert storage.data[(hash_, 6)] == b'abcdef'


@mock.patch.object(service, 'bytestream_pb2_grpc', autospec=True)
def test_bytestream_write_rejects_wrong_hash(mocked):
    storage = SimpleStorage()

    bs_instance = ByteStreamInstance(storage)
    servicer = ByteStreamService(server)
    servicer.add_instance("", bs_instance)

    data = b'some data'
    wrong_hash = HASH(b'incorrect').hexdigest()
    resource_name = "uploads/UUID-HERE/blobs/{}/9".format(wrong_hash)
    requests = [
        bytestream_pb2.WriteRequest(resource_name=resource_name, data=data, finish_write=True)
    ]

    servicer.Write(requests, context)
    context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)

    assert len(storage.data) is 0


@pytest.mark.parametrize("instance", instances)
@mock.patch.object(service, 'remote_execution_pb2_grpc', autospec=True)
def test_cas_find_missing_blobs(mocked, instance):
    storage = SimpleStorage([b'abc', b'def'])
    cas_instance = ContentAddressableStorageInstance(storage)
    servicer = ContentAddressableStorageService(server)
    servicer.add_instance(instance, cas_instance)

    digests = [
        re_pb2.Digest(hash=HASH(b'def').hexdigest(), size_bytes=3),
        re_pb2.Digest(hash=HASH(b'ghij').hexdigest(), size_bytes=4)
    ]
    request = re_pb2.FindMissingBlobsRequest(instance_name=instance, blob_digests=digests)
    response = servicer.FindMissingBlobs(request, context)
    assert len(response.missing_blob_digests) == 1
    assert response.missing_blob_digests[0] == digests[1]


@pytest.mark.parametrize("instance", instances)
@mock.patch.object(service, 'remote_execution_pb2_grpc', autospec=True)
def test_cas_batch_update_blobs(mocked, instance):
    storage = SimpleStorage()

    cas_instance = ContentAddressableStorageInstance(storage)
    servicer = ContentAddressableStorageService(server)
    servicer.add_instance(instance, cas_instance)

    update_requests = [
        re_pb2.BatchUpdateBlobsRequest.Request(
            digest=re_pb2.Digest(hash=HASH(b'abc').hexdigest(), size_bytes=3), data=b'abc'),
        re_pb2.BatchUpdateBlobsRequest.Request(
            digest=re_pb2.Digest(hash="invalid digest!", size_bytes=1000),
            data=b'wrong data')
    ]

    request = re_pb2.BatchUpdateBlobsRequest(instance_name=instance, requests=update_requests)
    response = servicer.BatchUpdateBlobs(request, context)
    assert len(response.responses) == 2

    for blob_response in response.responses:
        if blob_response.digest == update_requests[0].digest:
            assert blob_response.status.code == 0

        elif blob_response.digest == update_requests[1].digest:
            assert blob_response.status.code != 0

        else:
            raise Exception("Unexpected blob response")

    assert len(storage.data) == 1
    assert (update_requests[0].digest.hash, 3) in storage.data
    assert storage.data[(update_requests[0].digest.hash, 3)] == b'abc'
