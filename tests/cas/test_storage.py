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

import tempfile

import boto3
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from moto import mock_s3
import pytest

from buildgrid.server.cas.storage.storage_abc import StorageABC
from buildgrid.server.cas.storage.lru_memory_cache import LRUMemoryCache
from buildgrid.server.cas.storage.disk import DiskStorage
from buildgrid.server.cas.storage.s3 import S3Storage
from buildgrid.server.cas.storage.with_cache import WithCacheStorage
from buildgrid.settings import HASH

abc = b"abc"
abc_digest = Digest(hash=HASH(abc).hexdigest(), size_bytes=3)
defg = b"defg"
defg_digest = Digest(hash=HASH(defg).hexdigest(), size_bytes=4)
hijk = b"hijk"
hijk_digest = Digest(hash=HASH(hijk).hexdigest(), size_bytes=4)


def write(storage, digest, blob):
    session = storage.begin_write(digest)
    session.write(blob)
    storage.commit_write(digest, session)


# General tests for all storage providers


@pytest.fixture(params=["lru", "disk", "s3", "lru_disk", "disk_s3"])
def any_storage(request):
    if request.param == "lru":
        yield LRUMemoryCache(256)
    elif request.param == "disk":
        with tempfile.TemporaryDirectory() as path:
            yield DiskStorage(path)
    elif request.param == "s3":
        with mock_s3():
            boto3.resource('s3').create_bucket(Bucket="testing")
            yield S3Storage("testing")
    elif request.param == "lru_disk":
        # LRU cache with a uselessly small limit, so requests always fall back
        with tempfile.TemporaryDirectory() as path:
            yield WithCacheStorage(LRUMemoryCache(1), DiskStorage(path))
    elif request.param == "disk_s3":
        # Disk-based cache of S3, but we don't delete files, so requests
        # are always handled by the cache
        with tempfile.TemporaryDirectory() as path:
            with mock_s3():
                boto3.resource('s3').create_bucket(Bucket="testing")
                yield WithCacheStorage(DiskStorage(path), S3Storage("testing"))


def test_initially_empty(any_storage):
    assert not any_storage.has_blob(abc_digest)
    assert not any_storage.has_blob(defg_digest)
    assert not any_storage.has_blob(hijk_digest)


def test_basic_write_read(any_storage):
    assert not any_storage.has_blob(abc_digest)
    write(any_storage, abc_digest, abc)
    assert any_storage.has_blob(abc_digest)
    assert any_storage.get_blob(abc_digest).read() == abc

    # Try writing the same digest again (since it's valid to do that)
    write(any_storage, abc_digest, abc)
    assert any_storage.has_blob(abc_digest)
    assert any_storage.get_blob(abc_digest).read() == abc


def test_bulk_write_read(any_storage):
    missing_digests = any_storage.missing_blobs([abc_digest, defg_digest, hijk_digest])
    assert len(missing_digests) == 3
    assert abc_digest in missing_digests
    assert defg_digest in missing_digests
    assert hijk_digest in missing_digests

    bulk_update_results = any_storage.bulk_update_blobs([(abc_digest, abc), (defg_digest, defg),
                                                         (hijk_digest, b'????')])
    assert len(bulk_update_results) == 3
    assert bulk_update_results[0].code == 0
    assert bulk_update_results[1].code == 0
    assert bulk_update_results[2].code != 0

    missing_digests = any_storage.missing_blobs([abc_digest, defg_digest, hijk_digest])
    assert missing_digests == [hijk_digest]

    assert any_storage.get_blob(abc_digest).read() == abc
    assert any_storage.get_blob(defg_digest).read() == defg


def test_nonexistent_read(any_storage):
    assert any_storage.get_blob(abc_digest) is None


# Tests for special behavior of individual storage providers


def test_lru_eviction():
    lru = LRUMemoryCache(8)
    write(lru, abc_digest, abc)
    write(lru, defg_digest, defg)
    assert lru.has_blob(abc_digest)
    assert lru.has_blob(defg_digest)

    write(lru, hijk_digest, hijk)
    # Check that the LRU evicted abc (it was written first)
    assert not lru.has_blob(abc_digest)
    assert lru.has_blob(defg_digest)
    assert lru.has_blob(hijk_digest)

    assert lru.get_blob(defg_digest).read() == defg
    write(lru, abc_digest, abc)
    # Check that the LRU evicted hijk (since we just read defg)
    assert lru.has_blob(abc_digest)
    assert lru.has_blob(defg_digest)
    assert not lru.has_blob(hijk_digest)

    assert lru.has_blob(defg_digest)
    write(lru, hijk_digest, abc)
    # Check that the LRU evicted abc (since we just checked hijk)
    assert not lru.has_blob(abc_digest)
    assert lru.has_blob(defg_digest)
    assert lru.has_blob(hijk_digest)


def test_with_cache():
    cache = LRUMemoryCache(256)
    fallback = LRUMemoryCache(256)
    with_cache_storage = WithCacheStorage(cache, fallback)

    assert not with_cache_storage.has_blob(abc_digest)
    write(with_cache_storage, abc_digest, abc)
    assert cache.has_blob(abc_digest)
    assert fallback.has_blob(abc_digest)
    assert with_cache_storage.get_blob(abc_digest).read() == abc

    # Even if a blob is in cache, we still need to check if the fallback
    # has it.
    write(cache, defg_digest, defg)
    assert not with_cache_storage.has_blob(defg_digest)
    write(fallback, defg_digest, defg)
    assert with_cache_storage.has_blob(defg_digest)

    # When a blob is in the fallback but not the cache, reading it should
    # put it into the cache.
    write(fallback, hijk_digest, hijk)
    assert with_cache_storage.get_blob(hijk_digest).read() == hijk
    assert cache.has_blob(hijk_digest)
    assert cache.get_blob(hijk_digest).read() == hijk
    assert cache.has_blob(hijk_digest)
