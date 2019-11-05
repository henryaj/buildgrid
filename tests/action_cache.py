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


import boto3
import grpc
import pytest

from buildgrid._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.server.actioncache.instance import ActionCache
from buildgrid.server.actioncache.remote import RemoteActionCache
from buildgrid.server.actioncache.s3storage import S3ActionCache
from buildgrid.server.cas.storage import lru_memory_cache
from moto import mock_s3

from .utils.action_cache import serve_cache
from .utils.utils import run_in_subprocess


@pytest.fixture
def cas():
    return lru_memory_cache.LRUMemoryCache(1024 * 1024)


def test_null_cas_action_cache(cas):
    cache = ActionCache(cas, 0)

    action_digest1 = remote_execution_pb2.Digest(hash='alpha', size_bytes=4)
    dummy_result = remote_execution_pb2.ActionResult()

    cache.update_action_result(action_digest1, dummy_result)
    with pytest.raises(NotFoundError):
        cache.get_action_result(action_digest1)


def test_expiry(cas):
    cache = ActionCache(cas, 2)

    action_digest1 = remote_execution_pb2.Digest(hash='alpha', size_bytes=4)
    action_digest2 = remote_execution_pb2.Digest(hash='bravo', size_bytes=4)
    action_digest3 = remote_execution_pb2.Digest(hash='charlie', size_bytes=4)
    dummy_result = remote_execution_pb2.ActionResult()

    cache.update_action_result(action_digest1, dummy_result)
    cache.update_action_result(action_digest2, dummy_result)

    # Get digest 1 (making 2 the least recently used)
    assert cache.get_action_result(action_digest1) is not None
    # Add digest 3 (so 2 gets removed from the cache)
    cache.update_action_result(action_digest3, dummy_result)

    assert cache.get_action_result(action_digest1) is not None
    with pytest.raises(NotFoundError):
        cache.get_action_result(action_digest2)

    assert cache.get_action_result(action_digest3) is not None


@pytest.mark.parametrize('acType', ['memory', 's3'])
@mock_s3
def test_checks_cas(acType, cas):
    if acType == 'memory':
        cache = ActionCache(cas, 50)
    elif acType == 's3':
        auth_args = {"aws_access_key_id": "access_key",
                     "aws_secret_access_key": "secret_key"}
        boto3.resource('s3', **auth_args).create_bucket(Bucket='cachebucket')
        cache = S3ActionCache(cas, allow_updates=True, cache_failed_actions=True, bucket='cachebucket',
                              access_key="access_key", secret_key="secret_key")

    action_digest1 = remote_execution_pb2.Digest(hash='alpha', size_bytes=4)
    action_digest2 = remote_execution_pb2.Digest(hash='bravo', size_bytes=4)
    action_digest3 = remote_execution_pb2.Digest(hash='charlie', size_bytes=4)

    # Create a tree that actions digests in CAS
    sample_digest = cas.put_message(remote_execution_pb2.Command(arguments=["sample"]))
    tree = remote_execution_pb2.Tree()
    tree.root.files.add().digest.CopyFrom(sample_digest)
    tree.children.add().files.add().digest.CopyFrom(sample_digest)
    tree_digest = cas.put_message(tree)

    # Add an ActionResult that actions real digests to the cache
    action_result1 = remote_execution_pb2.ActionResult()
    action_result1.output_directories.add().tree_digest.CopyFrom(tree_digest)
    action_result1.output_files.add().digest.CopyFrom(sample_digest)
    action_result1.stdout_digest.CopyFrom(sample_digest)
    action_result1.stderr_digest.CopyFrom(sample_digest)
    cache.update_action_result(action_digest1, action_result1)

    # Add ActionResults that action fake digests to the cache
    action_result2 = remote_execution_pb2.ActionResult()
    action_result2.output_directories.add().tree_digest.hash = "nonexistent"
    action_result2.output_directories[0].tree_digest.size_bytes = 8
    cache.update_action_result(action_digest2, action_result2)

    action_result3 = remote_execution_pb2.ActionResult()
    action_result3.stdout_digest.hash = "nonexistent"
    action_result3.stdout_digest.size_bytes = 8
    cache.update_action_result(action_digest3, action_result3)

    # Verify we can get the first ActionResult but not the others
    fetched_result1 = cache.get_action_result(action_digest1)
    assert fetched_result1.output_directories[0].tree_digest.hash == tree_digest.hash
    with pytest.raises(NotFoundError):
        cache.get_action_result(action_digest2)
        cache.get_action_result(action_digest3)


def test_remote_update():

    def __test_update():
        with serve_cache(['testing']) as server:
            channel = grpc.insecure_channel(server.remote)
            cache = RemoteActionCache(channel, 'testing')

            action_digest = remote_execution_pb2.Digest(hash='alpha', size_bytes=4)
            result = remote_execution_pb2.ActionResult()
            cache.update_action_result(action_digest, result)

            fetched = cache.get_action_result(action_digest)
            assert result == fetched

    def __test_remote_update(queue):
        try:
            __test_update()
        except AssertionError:
            queue.put(False)
        else:
            queue.put(True)

    run_in_subprocess(__test_remote_update)


def test_remote_update_disallowed():

    def __test_update_disallowed():
        with serve_cache(['testing'], allow_updates=False) as server:
            channel = grpc.insecure_channel(server.remote)
            cache = RemoteActionCache(channel, 'testing')

            action_digest = remote_execution_pb2.Digest(hash='alpha', size_bytes=4)
            result = remote_execution_pb2.ActionResult()
            with pytest.raises(NotImplementedError, match='Updating cache not allowed'):
                cache.update_action_result(action_digest, result)

    def __test_remote_update_disallowed(queue):
        try:
            __test_update_disallowed()
        except AssertionError:
            queue.put(False)
        else:
            queue.put(True)

    run_in_subprocess(__test_remote_update_disallowed)


def test_remote_get_missing():

    def __test_get_missing():
        with serve_cache(['testing']) as server:
            channel = grpc.insecure_channel(server.remote)
            cache = RemoteActionCache(channel, 'testing')

            action_digest = remote_execution_pb2.Digest(hash='alpha', size_bytes=4)
            with pytest.raises(NotFoundError):
                cache.get_action_result(action_digest)

    def __test_remote_get_missing(queue):
        try:
            __test_get_missing()
        except AssertionError:
            queue.put(False)
        else:
            queue.put(True)

    run_in_subprocess(__test_remote_get_missing)
