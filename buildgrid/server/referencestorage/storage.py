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
Reference Cache
==================

Implements an in-memory reference cache.

For a given key, it
"""

import collections

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2

from .._exceptions import NotFoundError


class ReferenceCache:

    def __init__(self, storage, max_cached_refs, allow_updates=True):
        """ Initialises a new ReferenceCache instance.

        Args:
            storage (StorageABC): storage backend instance to be used.
            max_cached_refs (int): maximum number of entries to be stored.
            allow_updates (bool): allow the client to write to storage
        """
        self._allow_updates = allow_updates
        self._storage = storage
        self._max_cached_refs = max_cached_refs
        self._digest_map = collections.OrderedDict()

    def register_instance_with_server(self, instance_name, server):
        server.add_reference_storage_instance(self, instance_name)

    @property
    def allow_updates(self):
        return self._allow_updates

    def get_digest_reference(self, key):
        """Retrieves the cached Digest for the given key.

        Args:
            key: key for Digest to query.

        Returns:
            The cached Digest matching the given key or raises
            NotFoundError.
        """
        if key in self._digest_map:
            reference_result = self._storage.get_message(self._digest_map[key], remote_execution_pb2.Digest)

            if reference_result is not None:
                return reference_result

            del self._digest_map[key]

        raise NotFoundError("Key not found: {}".format(key))

    def get_action_reference(self, key):
        """Retrieves the cached ActionResult for the given Action digest.

        Args:
            key: key for ActionResult to query.

        Returns:
            The cached ActionResult matching the given key or raises
            NotFoundError.
        """
        if key in self._digest_map:
            reference_result = self._storage.get_message(self._digest_map[key], remote_execution_pb2.ActionResult)

            if reference_result is not None:
                if self._action_result_blobs_still_exist(reference_result):
                    self._digest_map.move_to_end(key)
                    return reference_result

            del self._digest_map[key]

        raise NotFoundError("Key not found: {}".format(key))

    def update_reference(self, key, result):
        """Stores the result in cache for the given key.

        If the cache size limit has been reached, the oldest cache entries will
        be dropped before insertion so that the cache size never exceeds the
        maximum numbers of entries allowed.

        Args:
            key: key to store result.
            result (Digest): result digest to store.
        """
        if not self._allow_updates:
            raise NotImplementedError("Updating cache not allowed")

        if self._max_cached_refs == 0:
            return

        while len(self._digest_map) >= self._max_cached_refs:
            self._digest_map.popitem(last=False)

        result_digest = self._storage.put_message(result)
        self._digest_map[key] = result_digest

    def _action_result_blobs_still_exist(self, action_result):
        """Checks CAS for ActionResult output blobs existance.

        Args:
            action_result (ActionResult): ActionResult to search referenced
            output blobs for.

        Returns:
            True if all referenced blobs are present in CAS, False otherwise.
        """
        blobs_needed = []

        for output_file in action_result.output_files:
            blobs_needed.append(output_file.digest)

        for output_directory in action_result.output_directories:
            blobs_needed.append(output_directory.tree_digest)
            tree = self._storage.get_message(output_directory.tree_digest,
                                             remote_execution_pb2.Tree)
            if tree is None:
                return False

            for file_node in tree.root.files:
                blobs_needed.append(file_node.digest)

            for child in tree.children:
                for file_node in child.files:
                    blobs_needed.append(file_node.digest)

        if action_result.stdout_digest.hash and not action_result.stdout_raw:
            blobs_needed.append(action_result.stdout_digest)

        if action_result.stderr_digest.hash and not action_result.stderr_raw:
            blobs_needed.append(action_result.stderr_digest)

        missing = self._storage.missing_blobs(blobs_needed)
        return len(missing) == 0
