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

import io

from google.bytestream import bytestream_pb2
from google.devtools.remoteexecution.v1test import remote_execution_pb2 as re_pb2
import pytest

from buildgrid.server.cas.storage.storage_abc import StorageABC
from buildgrid.server.cas.bytestream_service import ByteStreamService
from buildgrid.server.cas.content_addressable_storage_service import ContentAddressableStorageService
from buildgrid.settings import HASH


class SimpleStorage(StorageABC):
    """Storage provider wrapper around a dictionary.

    Does not attempt to delete old entries, so this is only useful for testing.
    """

    def __init__(self, existing_data=[]):
        self.data = {}
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


class MockObject:
    pass


class MockException(Exception):
    pass


def raise_mock_exception(*args, **kwargs):
    raise MockException()


test_strings = [b"", b"hij"] #, b"testing!" * 1000000]
instances = ["", "test_inst"]


@pytest.mark.parametrize("data_to_read", test_strings)
@pytest.mark.parametrize("instance", instances)
def test_bytestream_read(data_to_read, instance):
    storage = SimpleStorage([b"abc", b"defg", data_to_read])
    servicer = ByteStreamService(storage)

    request = bytestream_pb2.ReadRequest()
    if instance != "":
        request.resource_name = instance + "/"
    request.resource_name += f"blobs/{HASH(data_to_read).hexdigest()}/{len(data_to_read)}"

    data = b""
    for response in servicer.Read(request, None):
        data += response.data
    assert data == data_to_read


@pytest.mark.parametrize("instance", instances)
@pytest.mark.parametrize("extra_data", ["", "/", "/extra/data"])
def test_bytestream_write(instance, extra_data):
    storage = SimpleStorage()
    servicer = ByteStreamService(storage)

    resource_name = ""
    if instance != "":
        resource_name = instance + "/"
    hash_ = HASH(b'abcdef').hexdigest()
    resource_name += f"uploads/UUID-HERE/blobs/{hash_}/6"
    resource_name += extra_data
    requests = [
        bytestream_pb2.WriteRequest(resource_name=resource_name, data=b'abc'),
        bytestream_pb2.WriteRequest(data=b'def', write_offset=3, finish_write=True)
    ]

    response = servicer.Write(requests, None)
    assert response.committed_size == 6
    assert len(storage.data) == 1
    assert (hash_, 6) in storage.data
    assert storage.data[(hash_, 6)] == b'abcdef'


def test_bytestream_write_rejects_wrong_hash():
    storage = SimpleStorage()
    servicer = ByteStreamService(storage)

    data = b'some data'
    wrong_hash = HASH(b'incorrect').hexdigest()
    resource_name = f"uploads/UUID-HERE/blobs/{wrong_hash}/9"
    requests = [
        bytestream_pb2.WriteRequest(resource_name=resource_name, data=data, finish_write=True)
    ]

    context = MockObject()
    context.abort = raise_mock_exception
    with pytest.raises(MockException):
        servicer.Write(requests, context)
    assert len(storage.data) == 0


@pytest.mark.parametrize("instance", instances)
def test_cas_find_missing_blobs(instance):
    storage = SimpleStorage([b'abc', b'def'])
    servicer = ContentAddressableStorageService(storage)
    digests = [
        re_pb2.Digest(hash=HASH(b'def').hexdigest(), size_bytes=3),
        re_pb2.Digest(hash=HASH(b'ghij').hexdigest(), size_bytes=4)
    ]
    request = re_pb2.FindMissingBlobsRequest(instance_name=instance, blob_digests=digests)
    response = servicer.FindMissingBlobs(request, None)
    assert len(response.missing_blob_digests) == 1
    assert response.missing_blob_digests[0] == digests[1]


@pytest.mark.parametrize("instance", instances)
def test_cas_batch_update_blobs(instance):
    storage = SimpleStorage()
    servicer = ContentAddressableStorageService(storage)
    update_requests = [
        re_pb2.UpdateBlobRequest(
            content_digest=re_pb2.Digest(hash=HASH(b'abc').hexdigest(), size_bytes=3), data=b'abc'),
        re_pb2.UpdateBlobRequest(
            content_digest=re_pb2.Digest(hash="invalid digest!", size_bytes=1000),
            data=b'wrong data')
    ]
    request = re_pb2.BatchUpdateBlobsRequest(instance_name=instance, requests=update_requests)
    response = servicer.BatchUpdateBlobs(request, None)
    assert len(response.responses) == 2
    for blob_response in response.responses:
        if blob_response.blob_digest == update_requests[0].content_digest:
            assert blob_response.status.code == 0
        elif blob_response.blob_digest == update_requests[1].content_digest:
            assert blob_response.status.code != 0
        else:
            raise Exception("Unexpected blob response")
    assert len(storage.data) == 1
    assert (update_requests[0].content_digest.hash, 3) in storage.data
    assert storage.data[(update_requests[0].content_digest.hash, 3)] == b'abc'
