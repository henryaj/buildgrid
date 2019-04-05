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


import pytest

from buildgrid._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.server.actioncache.instance import ActionCache
from buildgrid.server.cas.storage import lru_memory_cache


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


def test_checks_cas(cas):
    cache = ActionCache(cas, 50)

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
