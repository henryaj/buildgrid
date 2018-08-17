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
Action Cache
============

Implements an in-memory action Cache
"""


from ..cas.reference_cache import ReferenceCache


class ActionCache(ReferenceCache):

    def get_action_result(self, action_digest):
        key = self._get_key(action_digest)
        return self.get_action_reference(key)

    def update_action_result(self, action_digest, action_result):
        key = self._get_key(action_digest)
        self.update_reference(key, action_result)

    def _get_key(self, action_digest):
        return (action_digest.hash, action_digest.size_bytes)
