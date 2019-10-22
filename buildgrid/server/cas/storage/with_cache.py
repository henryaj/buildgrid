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


"""
WithCacheStorage
==================

A storage provider that first checks a cache, then tries a slower
fallback provider.

To ensure clients can reliably store blobs in CAS, only `get_blob`
calls are cached -- `has_blob` and `missing_blobs` will always query
the fallback.
"""

import io
import logging

from .storage_abc import StorageABC


class _OutputTee(io.BufferedIOBase):
    """A file-like object that writes data to two file-like objects.

    The files should be in blocking mode; non-blocking mode is unsupported.
    """

    def __init__(self, file_a, file_b):
        super().__init__()

        self._original_a = file_a
        if isinstance(file_a, io.BufferedIOBase):
            self._a = file_a
        else:
            self._a = io.BufferedWriter(file_a)

        self._original_b = file_b
        if isinstance(file_b, io.BufferedIOBase):
            self._b = file_b
        else:
            self._b = io.BufferedWriter(file_b)

    def close(self):
        super().close()
        self._a.close()
        self._b.close()

    def flush(self):
        self._a.flush()
        self._b.flush()

    def readable(self):
        return False

    def seekable(self):
        return False

    def write(self, b):
        self._a.write(b)
        return self._b.write(b)

    def writable(self):
        return True


class _CachingTee(io.RawIOBase):
    """A file-like object that wraps a 'fallback' file, and when it's
    read, writes the resulting data to a 'cache' storage provider.

    Does not support non-blocking mode.
    """

    def __init__(self, fallback_file, digest, cache):
        super().__init__()

        self._file = fallback_file
        self._digest = digest
        self._cache = cache
        self._cache_session = cache.begin_write(digest)

    def close(self):
        super().close()
        self._cache_session.write(self._file.read())
        self._cache.commit_write(self._digest, self._cache_session)
        self._file.close()

    def readable(self):
        return True

    def seekable(self):
        return False

    def writable(self):
        return False

    def readall(self):
        data = self._file.read()
        self._cache_session.write(data)
        return data

    def readinto(self, b):
        bytes_read = self._file.readinto(b)
        self._cache_session.write(b[:bytes_read])
        return bytes_read


class WithCacheStorage(StorageABC):

    def __init__(self, cache, fallback):
        self.__logger = logging.getLogger(__name__)

        self._cache = cache
        self._fallback = fallback

    def has_blob(self, digest):
        return self._fallback.has_blob(digest)

    def get_blob(self, digest):
        cache_result = self._cache.get_blob(digest)
        if cache_result is not None:
            return cache_result
        fallback_result = self._fallback.get_blob(digest)
        if fallback_result is None:
            return None
        return _CachingTee(fallback_result, digest, self._cache)

    def delete_blob(self, digest):
        self._fallback.delete_blob(digest)
        self._cache.delete_blob(digest)

    def begin_write(self, digest):
        return _OutputTee(self._cache.begin_write(digest), self._fallback.begin_write(digest))

    def commit_write(self, digest, write_session):
        write_session.flush()
        self._cache.commit_write(digest, write_session._original_a)
        self._fallback.commit_write(digest, write_session._original_b)

    def missing_blobs(self, blobs):
        return self._fallback.missing_blobs(blobs)

    def bulk_update_blobs(self, blobs):
        self._cache.bulk_update_blobs(blobs)
        return self._fallback.bulk_update_blobs(blobs)
