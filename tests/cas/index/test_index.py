# Copyright (C) 2019 Bloomberg LP
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

import random
import tempfile
import time

import pytest

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.server.cas.storage.lru_memory_cache import LRUMemoryCache
from buildgrid.server.cas.storage.index.sql import SQLIndex
from buildgrid.settings import HASH

BLOBS = [b'abc', b'defg', b'hijk', b'']
DIGESTS = [remote_execution_pb2.Digest(hash=HASH(blob).hexdigest(),
                                       size_bytes=len(blob)) for blob in BLOBS]
EXTRA_BLOBS = [b'lmno', b'pqr']
EXTRA_DIGESTS = [remote_execution_pb2.Digest(
    hash=HASH(blob).hexdigest(),
    size_bytes=len(blob)) for blob in EXTRA_BLOBS]


@pytest.fixture()
def blobs_digests():
    return list(zip(BLOBS, DIGESTS))


@pytest.fixture()
def extra_blobs_digests():
    return list(zip(EXTRA_BLOBS, EXTRA_DIGESTS))


@pytest.fixture(params=['sql'])
def any_index(request):
    if request.param == 'sql':
        storage = LRUMemoryCache(256)
        with tempfile.NamedTemporaryFile() as db:
            yield SQLIndex(
                storage=storage,
                connection_string="sqlite:///%s" % db.name,
                automigrate=True)


def _write(storage, digest, blob):
    session = storage.begin_write(digest)
    session.write(blob)
    storage.commit_write(digest, session)


def test_has_blob(any_index, blobs_digests, extra_blobs_digests):
    """ The index should accurately reflect its contents with has_blob. """
    for _, digest in blobs_digests:
        assert any_index.has_blob(digest) is False
    for _, digest in extra_blobs_digests:
        assert any_index.has_blob(digest) is False

    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)

    for _, digest in blobs_digests:
        assert any_index.has_blob(digest) is True
    for _, digest in extra_blobs_digests:
        assert any_index.has_blob(digest) is False


def test_get_blob(any_index, blobs_digests, extra_blobs_digests):
    """ The index should properly return contents under get_blob. """
    for _, digest in blobs_digests:
        assert any_index.get_blob(digest) is None
    for _, digest in extra_blobs_digests:
        assert any_index.get_blob(digest) is None

    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)

    for blob, digest in blobs_digests:
        assert any_index.get_blob(digest).read() == blob
    for _, digest in extra_blobs_digests:
        assert any_index.get_blob(digest) is None


def test_timestamp_updated_by_get(any_index, blobs_digests):
    """ When a blob is accessed, the timestamp should be updated. """
    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)

    second_blob, second_digest = blobs_digests[1]

    old_order = []
    for digest in any_index.least_recent_digests():
        old_order.append(digest)

    assert second_digest == old_order[1]
    any_index.get_blob(second_digest)

    updated_order = []
    for digest in any_index.least_recent_digests():
        updated_order.append(digest)

    # Performing the get should have updated the timestamp
    assert second_digest == updated_order[-1]


def test_timestamp_updated_by_write(any_index, blobs_digests):
    """ Writes should also update the timestamp. """
    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)

    second_blob, second_digest = blobs_digests[1]

    old_order = []
    for digest in any_index.least_recent_digests():
        old_order.append(digest)

    assert second_digest == old_order[1]
    _write(any_index, second_digest, second_blob)

    updated_order = []
    for digest in any_index.least_recent_digests():
        updated_order.append(digest)

    # Performing the get should have updated the timestamp
    assert second_digest == updated_order[-1]


def _digestified_range(max):
    """ Generator for digests for bytestring representations of all numbers in
    the range [0, max)
    """
    for i in range(max):
        blob = bytes(i)
        yield remote_execution_pb2.Digest(
            hash=HASH(blob).hexdigest(),
            size_bytes=len(blob)
        )


def test_timestamp_updated_by_random_gets_and_writes(any_index):
    """ Test throwing a random arrangement of gets and writes against the
    index to ensure that it updates the blobs in the correct order.

    With an index primed with a modest number of blobs, we call get_blob
    on a third of them chosen randomly, _write on another third, and leave
    the last third alone. The index should be left in the proper state
    at the end."""
    digest_map = {}
    for i, digest in enumerate(_digestified_range(100)):
        _write(any_index, digest, bytes(i))
        digest_map[digest.hash] = i

    old_order = []
    for i, digest in enumerate(any_index.least_recent_digests()):
        assert digest_map[digest.hash] == i
        old_order.append(digest)

    untouched_digests = []
    updated_digests = []

    gets = ['get'] * (len(old_order) // 3)
    writes = ['write'] * (len(old_order) // 3)
    do_nothings = ['do nothing'] * (len(old_order) - len(gets) - len(writes))
    actions = gets + writes + do_nothings
    random.shuffle(actions)

    for i, (action, digest) in enumerate(zip(old_order, actions)):

        if action == 'get':
            any_index.get_blob(digest)
            updated_digests.append(digest)
        elif action == 'write':
            _write(any_index, digest, bytes(i))
            updated_digests.append(digest)
        elif action == 'do nothing':
            untouched_digests.append(digest)

    # The proper order should be every blob that wasn't updated (in relative
    # order), followed by every blob that was updated (in relative order)
    new_order = untouched_digests + updated_digests

    for actual_digest, expected_digest in zip(any_index.least_recent_digests(), new_order):
        assert actual_digest == expected_digest


def test_bulk_read_on_missing_blobs(any_index, blobs_digests, extra_blobs_digests):
    """ Check that attempting to do a bulk read on blobs that are missing
    returns the blobs for blobs found and None for all others.

    'extra_blobs_digests' will be our missing blobs in this test."""
    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)
    results = any_index.bulk_read_blobs(
        [digest for (blob, digest) in blobs_digests + extra_blobs_digests])
    for blob, digest in blobs_digests:
        assert results.get(digest.hash, None) is not None
    for blob, digest in extra_blobs_digests:
        assert results.get(digest.hash, None) is None


def _first_batch_seen_before_second(index, first_batch, second_batch):
    """ Returns true if the first batch of digests is located entirely before
    the second group in the index's LRU order.
    """
    first_batch_hashes = set([digest.hash for digest in first_batch])
    second_batch_hashes = set([digest.hash for digest in second_batch])

    # The sets must be disjoint
    assert(first_batch_hashes.isdisjoint(second_batch_hashes))

    for digest in index.least_recent_digests():
        # Delete the element if it's in the first set
        first_batch_hashes.discard(digest.hash)
        # If we see anything in the second set, we should have seen everything
        # in the first set
        if digest.hash in second_batch_hashes:
            return not first_batch_hashes
    return False


def test_timestamp_updated_by_bulk_update(any_index, blobs_digests, extra_blobs_digests):
    """ Test that timestamps are updated by bulk writes. """
    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)
    for blob, digest in extra_blobs_digests:
        _write(any_index, digest, blob)

    first_batch_digests = [digest for (blob, digest) in blobs_digests]
    second_batch_digests = [digest for (blob, digest) in extra_blobs_digests]

    # At the start, the blobs in the first batch have an earlier timestamp
    # than the blobs in the second
    assert _first_batch_seen_before_second(
        any_index, first_batch_digests, second_batch_digests)

    any_index.bulk_update_blobs([(digest, blob)
                                 for (blob, digest) in blobs_digests])

    # After rewriting the first batch, the first batch appears after the second
    assert _first_batch_seen_before_second(
        any_index, second_batch_digests, first_batch_digests)


def test_timestamp_updated_by_bulk_read(any_index, blobs_digests, extra_blobs_digests):
    """ Test that timestamps are updated by bulk reads. """
    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)
    for blob, digest in extra_blobs_digests:
        _write(any_index, digest, blob)

    first_batch_digests = [digest for (blob, digest) in blobs_digests]
    second_batch_digests = [digest for (blob, digest) in extra_blobs_digests]

    # At the start, the blobs in the first batch have an earlier timestamp
    # than the blobs in the second
    assert _first_batch_seen_before_second(
        any_index, first_batch_digests, second_batch_digests)

    any_index.bulk_read_blobs(first_batch_digests)

    # After reading the first batch, the first batch appears after the second
    assert _first_batch_seen_before_second(
        any_index, second_batch_digests, first_batch_digests)


def test_timestamp_updated_by_missing_blobs(any_index, blobs_digests):
    """ FindMissingBlobs should also update the timestamp. """
    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)

    second_blob, second_digest = blobs_digests[1]

    old_order = []
    for digest in any_index.least_recent_digests():
        old_order.append(digest)

    assert second_digest == old_order[1]
    any_index.missing_blobs([second_digest])

    updated_order = []
    for digest in any_index.least_recent_digests():
        updated_order.append(digest)

    # Performing the get should have updated the timestamp
    assert second_digest == updated_order[-1]


def test_large_missing_blobs(any_index):
    """ Ensure that a large missing_blobs query can be handled by the index
    implementation. We'll just use an empty index for this test since
    we don't really care about its state.

    SQLite can only handle 999 bind variables, so we'll test a search for
    a much larger number."""
    large_input = [digest for digest in _digestified_range(50000)]
    expected_hashes = {digest.hash for digest in large_input}
    for digest in any_index.missing_blobs(large_input):
        assert digest.hash in expected_hashes
        expected_hashes.remove(digest.hash)
    assert not expected_hashes


def test_large_missing_blobs_with_some_present(any_index):
    """ Same as above, but let's add some blobs to the index. """
    not_missing = set()
    for i, digest in enumerate(_digestified_range(1000)):
        _write(any_index, digest, bytes(i))
        not_missing.add(digest.hash)

    large_input = [digest for digest in _digestified_range(50000)]
    expected_hashes = {digest.hash for digest in large_input
                       if digest.hash not in not_missing}
    for digest in any_index.missing_blobs(large_input):
        assert digest.hash in expected_hashes
        expected_hashes.remove(digest.hash)
    assert not expected_hashes


def test_delete_blob(any_index, blobs_digests):
    """ Deleting a blob should make has_blob return False. """
    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)

    second_blob, second_digest = blobs_digests[1]

    assert any_index.has_blob(second_digest) is True

    any_index.delete_blob(second_digest)

    assert any_index.has_blob(second_digest) is False


def test_get_blob_fallback(any_index, blobs_digests):
    """ If fallback_on_get is enabled, blobs not present in the index should
    get pulled in from the backend storage if they're not in the index when
    get_blob is called. """

    for blob, digest in blobs_digests:
        # Write the blobs to the storage directly but not through the index
        _write(any_index._storage, digest, blob)

    second_blob, second_digest = blobs_digests[1]

    # First let's show what happens when fallback is disabled. The
    # digest isn't pulled into the index.
    any_index._fallback_on_get = False
    assert any_index.has_blob(second_digest) is False
    assert any_index.get_blob(second_digest) is None
    assert any_index.has_blob(second_digest) is False

    # Now let's enable fallback to demonstrate that the index will
    # contain the element after fetching
    any_index._fallback_on_get = True
    assert any_index.get_blob(second_digest).read() == second_blob
    assert any_index.has_blob(second_digest) is True


def test_duplicate_writes_ok(any_index, blobs_digests):
    for blob, digest in blobs_digests:
        _write(any_index, digest, blob)
        _write(any_index, digest, blob)
    for blob, digest in blobs_digests:
        assert any_index.has_blob(digest) is True
        assert any_index.get_blob(digest).read() == blob


def test_bulk_read_blobs_fallback(any_index, blobs_digests):
    """ Similar to the above, BatchUpdateBlobs should retrieve blobs from
    storage when fallback is enabled. """
    for blob, digest in blobs_digests:
        # Write the blobs to the storage directly but not through the index
        _write(any_index._storage, digest, blob)

    # To make things slightly more interesting we'll add the first blob
    # to the index
    first_blob, first_digest = blobs_digests[0]
    _write(any_index, first_digest, first_blob)

    digests = [digest for (blob, digest) in blobs_digests]

    # We'll first show what happens when fallback is disabled
    any_index._fallback_on_get = False
    for digest in digests:
        assert any_index.has_blob(digest) is (digest == first_digest)
    any_index.bulk_read_blobs(digests)
    for digest in digests:
        assert any_index.has_blob(digest) is (digest == first_digest)

    # Now with fallback. All of the blobs should be present after a read.
    any_index._fallback_on_get = True
    any_index.bulk_read_blobs(digests)
    for digest in digests:
        assert any_index.has_blob(digest) is True
