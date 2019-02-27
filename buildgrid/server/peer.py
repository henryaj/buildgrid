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

import logging
from threading import Lock

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.settings import REQUEST_METADATA_HEADER_NAME


class Peer:
    """Represents a client during a session."""

    # We will keep a global list of Peers indexed by their `Peer.uid`.
    __peers_by_uid = {}
    __peers_by_uid_lock = Lock()

    @classmethod
    def register_peer(cls, uid, context, token=None):
        """Registers a new peer from a given context.

        Args:
            uid (str): a unique identifier
            token (str): an authentication token (optional)
            context (grpc.ServicerContext): context in which the peer is
                being registered

        Returns:
            Peer: an existing or newly created Peer object
        """

        request_metadata = cls._context_extract_request_metadata(context)

        if request_metadata and request_metadata.tool_details:
            tool_name = request_metadata.tool_details.tool_name
            tool_version = request_metadata.tool_details.tool_version
        else:
            tool_name = tool_version = None

        with cls.__peers_by_uid_lock:
            # If the peer exists, we just increment the counter on the existing
            # instance:
            if uid in cls.__peers_by_uid:
                existing_peer = cls.__peers_by_uid[uid]
                logging.getLogger(__name__).debug('Registering another instance '
                                                  'of Peer with uid {} ', uid)
                existing_peer.__instance_count += 1
                return existing_peer
            else:
                # Otherwise we add ourselves to the list of Peers:
                new_peer = Peer(uid=uid, token=token, tool_name=tool_name,
                                tool_version=tool_version)
                cls.__peers_by_uid[uid] = new_peer
                return cls.__peers_by_uid[uid]

    def __init__(self, uid, token=None, tool_name=None, tool_version=None):
        """Creates a new Peer object.

        Args:
            uid (str): a unique identifier
            token (str): an authentication token (optional)
            tool_name(str): reported tool name for the peer's request
            tool_version(str): reported tool version for the peer's request
        """
        self._uid = uid  # This uniquely identifies a client
        self._token = token

        self._tool_name = tool_name
        self._tool_version = tool_version

        # Each Peer object contains the number of instances of itself:
        self.__instance_count = 1

    @classmethod
    def find_peer(cls, uid):
        return cls.__peers_by_uid.get(uid, None)

    def __eq__(self, other):
        if not isinstance(other, Peer):
            return False

        return self.uid == other.uid and self.token == other.token and \
            self.request_metadata == other.request_metadata

    def __hash__(self):
        return hash(self.uid)  # This string is unique for each peer

    def __str__(self):
        return 'Peer: uid: {}, tool_details: {} - {}'.format(self._uid,
                                                             self._tool_name,
                                                             self._tool_version)

    @property
    def uid(self):
        return self._uid

    @property
    def token(self):
        return self._token

    # -- `RequestMetadata` optional values (attached to the Execute() call) --
    @property
    def tool_name(self):
        return self._tool_name

    @property
    def tool_version(self):
        return self._tool_version

    @classmethod
    def deregister_peer(cls, peer_uid):
        """Deregisters a Peer from the list of peers present.
        If the Peer deregistered has a single instance, we delete it
        from the dictionary.
        """
        with cls.__peers_by_uid_lock:
            cls.__peers_by_uid[peer_uid].__instance_count -= 1

            if cls.__peers_by_uid[peer_uid].__instance_count < 1:
                del cls.__peers_by_uid[peer_uid]

    @classmethod
    def _context_extract_request_metadata(cls, context):
        """Given a `grpc.ServicerContext` object, extract the RequestMetadata
        header values if they are present. If they were not provided,
        returns None.

        Args:
            context (grpc.ServicerContext): Context for a RPC call.

        Returns:
            A `RequestMetadata` proto if RequestMetadata values are present,
            otherwise None.
        """
        invocation_metadata = context.invocation_metadata()
        request_metadata_entry = next((entry for entry in invocation_metadata
                                       if entry.key == REQUEST_METADATA_HEADER_NAME),
                                      None)
        if not request_metadata_entry:
            return None

        request_metadata = remote_execution_pb2.RequestMetadata()
        request_metadata.ParseFromString(request_metadata_entry.value)

        return request_metadata
