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

"""
LRUMemoryCache
==================

A storage provider that stores data in memory. When the size limit
is reached, items are deleted from the cache with the least recently
used item being deleted first.
"""

import collections
import io
import logging
import threading

from .storage_abc import StorageABC


class _NullBytesIO(io.BufferedIOBase):
    """A file-like object that discards all data written to it."""

    def writable(self):
        return True

    def write(self, b):
        return len(b)


class LRUMemoryCache(StorageABC):

    def __init__(self, limit):
        self._limit = limit
        self._storage = collections.OrderedDict()
        self._bytes_stored = 0
        self._lock = threading.Lock()
        self.logger = logging.getLogger(__name__)

    def has_blob(self, digest):
        with self._lock:
            key = (digest.hash, digest.size_bytes)
            result = key in self._storage
            if result:
                self._storage.move_to_end(key)
            return result

    def get_blob(self, digest):
        with self._lock:
            key = (digest.hash, digest.size_bytes)
            if key in self._storage:
                self._storage.move_to_end(key)
                return io.BytesIO(self._storage[key])
            return None

    def begin_write(self, digest):
        if digest.size_bytes > self._limit:
            # Don't try to cache objects bigger than our memory limit.
            return _NullBytesIO()
        return io.BytesIO()

    def commit_write(self, digest, write_session):
        if isinstance(write_session, _NullBytesIO):
            # We can't cache this object, so return without doing anything.
            return
        with self._lock:
            self._bytes_stored += digest.size_bytes

            if self._bytes_stored > self._limit:
                # Delete stuff until we're back under the limit.
                while self._bytes_stored > self._limit:
                    deleted_key = self._storage.popitem(last=False)[0]
                    self._bytes_stored -= deleted_key[1]

            key = (digest.hash, digest.size_bytes)
            self._storage[key] = write_session.getvalue()
