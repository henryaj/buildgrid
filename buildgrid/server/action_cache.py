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
ActionCache
==================

Implements a simple in-memory action cache.
"""

import collections

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2 as re_pb2


class ActionCache:

    def __init__(self, storage, max_cached_actions):
        self._storage = storage
        self._max_cached_actions = max_cached_actions
        self._digest_map = collections.OrderedDict()

    def get_action_result(self, action_digest):
        """Return the cached ActionResult for the given Action digest, or None
        if there isn't one.
        """
        key = (action_digest.hash, action_digest.size_bytes)
        if key in self._digest_map:
            action_result = self._storage.get_message(self._digest_map[key],
                                                      re_pb2.ActionResult)
            if action_result is not None:
                if self._blobs_still_exist(action_result):
                    self._digest_map.move_to_end(key)
                    return action_result
            del self._digest_map[key]
        return None

    def put_action_result(self, action_digest, action_result):
        """Add the given ActionResult to the cache for the given Action
        digest.
        """
        if self._max_cached_actions == 0:
            return

        while len(self._digest_map) >= self._max_cached_actions:
            self._digest_map.popitem(last=False)

        key = (action_digest.hash, action_digest.size_bytes)
        action_result_digest = self._storage.put_message(action_result)
        self._digest_map[key] = action_result_digest

    def _blobs_still_exist(self, action_result):
        """Return True if all the CAS blobs referenced by the given
        ActionResult are present in CAS.
        """
        blobs_needed = []

        for output_file in action_result.output_files:
            blobs_needed.append(output_file.digest)

        for output_directory in action_result.output_directories:
            blobs_needed.append(output_directory.tree_digest)
            tree = self._storage.get_message(output_directory.tree_digest,
                                             re_pb2.Tree)
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
