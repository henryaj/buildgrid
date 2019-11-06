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


"""
IndexABC
==================

The abstract base class for storage indices. An index is a special type of
Storage that facilitates storing blob metadata. It must wrap another Storage.

Derived classes must implement all methods of both this interface and the
StorageABC interface.
"""

import abc

from ..storage_abc import StorageABC


class IndexABC(StorageABC):

    @abc.abstractmethod
    def __init__(self, *, fallback_on_get=False):

        # If fallback is enabled, the index is required to fetch blobs from
        # storage on each get_blob and bulk_read_blobs request and update
        # itself accordingly.
        self._fallback_on_get = fallback_on_get

    @abc.abstractmethod
    def delete_blob(self, digest):
        """ Delete a blob from the index. Return True if the blob was deleted,
        or False otherwise.

        TODO: This method will be promoted to StorageABC in a future commit. """
        raise NotImplementedError()

    @abc.abstractmethod
    def least_recent_digests(self):
        """ Generator to iterate through the digests in LRU order """
        raise NotImplementedError()
