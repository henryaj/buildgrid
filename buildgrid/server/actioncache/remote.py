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
Remote Action Cache
===================

Provides an interface to a remote Action Cache. This is used by Execution
services to communicate with remote caches.

"""

import logging

from buildgrid._exceptions import NotFoundError
from buildgrid.client.actioncache import ActionCacheClient
from buildgrid.client.capabilities import CapabilitiesInterface


class RemoteActionCache:

    def __init__(self, channel, instance_name):
        self.__logger = logging.getLogger(__name__)
        self.channel = channel
        self.instance_name = instance_name
        self._allow_updates = None
        self._action_cache = ActionCacheClient(
            self.channel, instance=self.instance_name)

    @property
    def allow_updates(self):
        # Check if updates are allowed if we haven't already.
        # This is done the first time update_action_result is called rather
        # than on instantiation because the remote cache may not be running
        # when this object is instantiated.
        if self._allow_updates is None:
            interface = CapabilitiesInterface(self.channel)
            capabilities = interface.get_capabilities(self.instance_name)
            self._allow_updates = (capabilities
                                   .cache_capabilities
                                   .action_cache_update_capabilities
                                   .update_enabled)
        return self._allow_updates

    def get_action_result(self, action_digest):
        action_result = self._action_cache.get(action_digest)

        if action_result is None:
            key = self._get_key(action_digest)
            raise NotFoundError("Key not found: {}".format(key))
        return action_result

    def update_action_result(self, action_digest, action_result):
        if not self.allow_updates:
            raise NotImplementedError("Updating cache not allowed")
        return self._action_cache.update(action_digest, action_result)

    def _get_key(self, action_digest):
        return (action_digest.hash, action_digest.size_bytes)
