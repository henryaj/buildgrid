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


import tempfile

import boto3
import grpc
from moto import mock_s3
import pytest
import fakeredis
from unittest.mock import patch

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.server.cas.storage.storage_abc import StorageABC
from buildgrid.server.cas.storage.remote import RemoteStorage
from buildgrid.server.cas.storage.lru_memory_cache import LRUMemoryCache
from buildgrid.server.cas.storage.disk import DiskStorage
from buildgrid.server.cas.storage.s3 import S3Storage
from buildgrid.server.cas.storage.with_cache import WithCacheStorage
from buildgrid.server.cas.storage.index.sql import SQLIndex
from buildgrid.settings import HASH

from ..utils.cas import serve_cas
from ..utils.utils import run_in_subprocess


BLOBS = [(b'abc', b'defg', b'hijk', b'')]
BLOBS_DIGESTS = [tuple([remote_execution_pb2.Digest(hash=HASH(blob).hexdigest(),
                                                    size_bytes=len(blob)) for blob in blobs])
                 for blobs in BLOBS]


@pytest.fixture(params=['lru', 'disk', 's3', 'lru_disk', 'disk_s3', 'remote', 'redis', 'sql_index'])
def any_storage(request):
    if request.param == 'lru':
        yield LRUMemoryCache(256)
    elif request.param == 'disk':
        with tempfile.TemporaryDirectory() as path:
            yield DiskStorage(path)
    elif request.param == 's3':
        with mock_s3():
            auth_args = {"aws_access_key_id": "access_key",
                         "aws_secret_access_key": "secret_key"}
            boto3.resource('s3', **auth_args).create_bucket(Bucket='testing')
            yield S3Storage('testing')
    elif request.param == 'lru_disk':
        # LRU cache with a uselessly small limit, so requests always fall back
        with tempfile.TemporaryDirectory() as path:
            yield WithCacheStorage(LRUMemoryCache(1), DiskStorage(path))
    elif request.param == 'disk_s3':
        # Disk-based cache of S3, but we don't delete files, so requests
        # are always handled by the cache
        with tempfile.TemporaryDirectory() as path:
            with mock_s3():
                auth_args = {"aws_access_key_id": "access_key",
                             "aws_secret_access_key": "secret_key"}
                boto3.resource('s3', **auth_args).create_bucket(Bucket='testing')
                yield WithCacheStorage(DiskStorage(path), S3Storage('testing'))
    elif request.param == 'remote':
        with serve_cas(['testing']) as server:
            yield server.remote
    elif request.param == 'redis':
        with patch('buildgrid.server.cas.storage.redis.redis.Redis', fakeredis.FakeRedis):
            from buildgrid.server.cas.storage.redis import RedisStorage

            input_dict = {
                'host': "localhost",
                'port': 8000,
                'db': 0
            }
            yield RedisStorage(**input_dict)
    elif request.param == 'sql_index':
        storage = LRUMemoryCache(256)
        with tempfile.NamedTemporaryFile() as db:
            yield SQLIndex(
                storage=storage,
                connection_string="sqlite:///%s" % db.name,
                automigrate=True)


def write(storage, digest, blob):
    session = storage.begin_write(digest)
    session.write(blob)
    storage.commit_write(digest, session)


@pytest.mark.parametrize('blobs_digests', zip(BLOBS, BLOBS_DIGESTS))
def test_initially_empty(any_storage, blobs_digests):
    _, digests = blobs_digests

    # Actual test function, failing on assertions:
    def __test_initially_empty(any_storage, digests):
        for digest in digests:
            assert not any_storage.has_blob(digest)

    # Helper test function for remote storage, to be run in a subprocess:
    def __test_remote_initially_empty(queue, remote, serialized_digests):
        channel = grpc.insecure_channel(remote)
        remote_storage = RemoteStorage(channel, 'testing')
        digests = []

        for data in serialized_digests:
            digest = remote_execution_pb2.Digest()
            digest.ParseFromString(data)
            digests.append(digest)

        try:
            __test_initially_empty(remote_storage, digests)
        except AssertionError:
            queue.put(False)
        else:
            queue.put(True)

    if isinstance(any_storage, str):
        serialized_digests = [digest.SerializeToString() for digest in digests]
        assert run_in_subprocess(__test_remote_initially_empty,
                                 any_storage, serialized_digests)
    else:
        __test_initially_empty(any_storage, digests)


@pytest.mark.parametrize('blobs_digests', zip(BLOBS, BLOBS_DIGESTS))
def test_basic_write_read(any_storage, blobs_digests):
    blobs, digests = blobs_digests

    # Actual test function, failing on assertions:
    def __test_basic_write_read(any_storage, blobs, digests):
        for blob, digest in zip(blobs, digests):
            assert not any_storage.has_blob(digest)
            write(any_storage, digest, blob)
            assert any_storage.has_blob(digest)
            assert any_storage.get_blob(digest).read() == blob

            # Try writing the same digest again (since it's valid to do that)
            write(any_storage, digest, blob)
            assert any_storage.has_blob(digest)
            assert any_storage.get_blob(digest).read() == blob

    # Helper test function for remote storage, to be run in a subprocess:
    def __test_remote_basic_write_read(queue, remote, blobs, serialized_digests):
        channel = grpc.insecure_channel(remote)
        remote_storage = RemoteStorage(channel, 'testing')
        digests = []

        for data in serialized_digests:
            digest = remote_execution_pb2.Digest()
            digest.ParseFromString(data)
            digests.append(digest)

        try:
            __test_basic_write_read(remote_storage, blobs, digests)
        except AssertionError:
            queue.put(False)
        else:
            queue.put(True)

    if isinstance(any_storage, str):
        serialized_digests = [digest.SerializeToString() for digest in digests]
        assert run_in_subprocess(__test_remote_basic_write_read,
                                 any_storage, blobs, serialized_digests)
    else:
        __test_basic_write_read(any_storage, blobs, digests)


@pytest.mark.parametrize('blobs_digests', zip(BLOBS, BLOBS_DIGESTS))
def test_deletes(any_storage, blobs_digests):
    """ Test the functionality of deletes.

    Deleting a blob should cause has_blob to return False and
    get_blob to return None.
    """
    blobs, digests = blobs_digests
    # any_storage returns a string for remote storage. Since deletes
    # are not supported with remote storage, we ignore those
    if isinstance(any_storage, StorageABC):
        for blob, digest in zip(blobs, digests):
            write(any_storage, digest, blob)

        for blob, digest in zip(blobs, digests):
            assert any_storage.has_blob(digest)
            assert any_storage.get_blob(digest).read() == blob

        first_digest, *_ = digests

        any_storage.delete_blob(first_digest)

        for blob, digest in zip(blobs, digests):
            if digest != first_digest:
                assert any_storage.has_blob(digest)
                assert any_storage.get_blob(digest).read() == blob
            else:
                assert not any_storage.has_blob(digest)
                assert any_storage.get_blob(digest) is None

        # There shouldn't be any issue with deleting a blob that isn't there
        missing_digest = remote_execution_pb2.Digest(hash=HASH(b'missing_blob').hexdigest(),
                                                     size_bytes=len(b'missing_blob'))
        assert not any_storage.has_blob(missing_digest)
        assert any_storage.get_blob(missing_digest) is None
        any_storage.delete_blob(missing_digest)
        assert not any_storage.has_blob(missing_digest)
        assert any_storage.get_blob(missing_digest) is None


@pytest.mark.parametrize('blobs_digests', zip(BLOBS, BLOBS_DIGESTS))
def test_bulk_write_read(any_storage, blobs_digests):
    blobs, digests = blobs_digests

    # Actual test function, failing on assertions:
    def __test_bulk_write_read(any_storage, blobs, digests):
        missing_digests = any_storage.missing_blobs(digests)
        assert len(missing_digests) == len(digests)
        for digest in digests:
            assert digest in missing_digests

        faulty_blobs = list(blobs)
        faulty_blobs[-1] = b'this-is-not-matching'

        results = any_storage.bulk_update_blobs(list(zip(digests, faulty_blobs)))
        assert len(results) == len(digests)
        for result, blob, digest in zip(results[:-1], faulty_blobs[:-1], digests[:-1]):
            assert result.code == 0
            assert any_storage.get_blob(digest).read() == blob
        assert results[-1].code != 0

        missing_digests = any_storage.missing_blobs(digests)
        assert len(missing_digests) == 1
        assert missing_digests[0] == digests[-1]

    # Helper test function for remote storage, to be run in a subprocess:
    def __test_remote_bulk_write_read(queue, remote, blobs, serialized_digests):
        channel = grpc.insecure_channel(remote)
        remote_storage = RemoteStorage(channel, 'testing')
        digests = []

        for data in serialized_digests:
            digest = remote_execution_pb2.Digest()
            digest.ParseFromString(data)
            digests.append(digest)

        try:
            __test_bulk_write_read(remote_storage, blobs, digests)
        except AssertionError:
            queue.put(False)
        else:
            queue.put(True)

    if isinstance(any_storage, str):
        serialized_digests = [digest.SerializeToString() for digest in digests]
        assert run_in_subprocess(__test_remote_bulk_write_read,
                                 any_storage, blobs, serialized_digests)
    else:
        __test_bulk_write_read(any_storage, blobs, digests)


@pytest.mark.parametrize('blobs_digests', zip(BLOBS, BLOBS_DIGESTS))
def test_nonexistent_read(any_storage, blobs_digests):
    _, digests = blobs_digests

    # Actual test function, failing on assertions:
    def __test_nonexistent_read(any_storage, digests):
        for digest in digests:
            assert any_storage.get_blob(digest) is None

    # Helper test function for remote storage, to be run in a subprocess:
    def __test_remote_nonexistent_read(queue, remote, serialized_digests):
        channel = grpc.insecure_channel(remote)
        remote_storage = RemoteStorage(channel, 'testing')
        digests = []

        for data in serialized_digests:
            digest = remote_execution_pb2.Digest()
            digest.ParseFromString(data)
            digests.append(digest)

        try:
            __test_nonexistent_read(remote_storage, digests)
        except AssertionError:
            queue.put(False)
        else:
            queue.put(True)

    if isinstance(any_storage, str):
        serialized_digests = [digest.SerializeToString() for digest in digests]
        assert run_in_subprocess(__test_remote_nonexistent_read,
                                 any_storage, serialized_digests)
    else:
        __test_nonexistent_read(any_storage, digests)


@pytest.mark.parametrize('blobs_digests', [(BLOBS[0], BLOBS_DIGESTS[0])])
def test_lru_eviction(blobs_digests):
    blobs, digests = blobs_digests
    blob1, blob2, blob3, *_ = blobs
    digest1, digest2, digest3, *_ = digests

    lru = LRUMemoryCache(8)
    write(lru, digest1, blob1)
    write(lru, digest2, blob2)
    assert lru.has_blob(digest1)
    assert lru.has_blob(digest2)

    write(lru, digest3, blob3)
    # Check that the LRU evicted blob1 (it was written first)
    assert not lru.has_blob(digest1)
    assert lru.has_blob(digest2)
    assert lru.has_blob(digest3)

    assert lru.get_blob(digest2).read() == blob2
    write(lru, digest1, blob1)
    # Check that the LRU evicted blob3 (since we just read blob2)
    assert lru.has_blob(digest1)
    assert lru.has_blob(digest2)
    assert not lru.has_blob(digest3)

    assert lru.has_blob(digest2)
    write(lru, digest3, blob1)
    # Check that the LRU evicted blob1 (since we just checked blob3)
    assert not lru.has_blob(digest1)
    assert lru.has_blob(digest2)
    assert lru.has_blob(digest3)


@pytest.mark.parametrize('blobs_digests', [(BLOBS[0], BLOBS_DIGESTS[0])])
def test_with_cache(blobs_digests):
    blobs, digests = blobs_digests
    blob1, blob2, blob3, *_ = blobs
    digest1, digest2, digest3, *_ = digests

    cache = LRUMemoryCache(256)
    fallback = LRUMemoryCache(256)
    with_cache_storage = WithCacheStorage(cache, fallback)

    assert not with_cache_storage.has_blob(digest1)
    write(with_cache_storage, digest1, blob1)
    assert cache.has_blob(digest1)
    assert fallback.has_blob(digest1)
    assert with_cache_storage.get_blob(digest1).read() == blob1

    # Even if a blob is in cache, we still need to check if the fallback
    # has it.
    write(cache, digest2, blob2)
    assert not with_cache_storage.has_blob(digest2)
    write(fallback, digest2, blob2)
    assert with_cache_storage.has_blob(digest2)

    # When a blob is in the fallback but not the cache, reading it should
    # put it into the cache.
    write(fallback, digest3, blob3)
    assert with_cache_storage.get_blob(digest3).read() == blob3
    assert cache.has_blob(digest3)
    assert cache.get_blob(digest3).read() == blob3
    assert cache.has_blob(digest3)
