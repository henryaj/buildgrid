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

import grpc

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.server.peer import Peer
from buildgrid.settings import REQUEST_METADATA_HEADER_NAME


class DummyServicerContext(grpc.ServicerContext):
    """Dummy context object passed to method implementations."""

    def invocation_metadata(self):
        return {}

    def peer(self):
        raise NotImplementedError()

    def peer_identities(self):
        raise NotImplementedError()

    def peer_identity_key(self):
        raise NotImplementedError()

    def auth_context(self):
        raise NotImplementedError()

    def send_initial_metadata(self, initial_metadata):
        raise NotImplementedError()

    def set_trailing_metadata(self, trailing_metadata):
        raise NotImplementedError()

    def abort(self, code, details):
        raise NotImplementedError()

    def set_code(self, code):
        raise NotImplementedError()

    def set_details(self, details):
        raise NotImplementedError()

    def abort_with_status(self, status):
        raise NotImplementedError()

    def add_callback(self, callback):
        raise NotImplementedError()

    def cancel(self):
        raise NotImplementedError()

    def is_active(self):
        raise NotImplementedError()

    def time_remaining(self):
        raise NotImplementedError()


context = DummyServicerContext()


def test_empty_peer_list():
    assert Peer.find_peer('peer0') is None


def test_create_new_peer_object():
    p = Peer(uid='peer1')

    assert p.uid == 'peer1'
    assert p.token is None
    assert p.tool_name is None
    assert p.tool_name is None


def test_register_new_peer_in_global_list():
    p = Peer.register_peer(uid='peer1', context=context)

    assert p.uid == 'peer1'
    assert Peer.find_peer('peer1') is p


def test_peer_metadata():
    peer_uid = 'peer2'

    tool_name = 'tool-name'
    tool_version = '1.0'

    class DummyServicerContextWithMetadata(DummyServicerContext):
        def invocation_metadata(self):
            request_metadata = remote_execution_pb2.RequestMetadata()
            request_metadata.tool_details.tool_name = tool_name
            request_metadata.tool_details.tool_version = tool_version

            request_metadata.action_id = '920ea86d6a445df893d0a39815e8856254392ce40e5957f167af8f16485916fb'
            request_metadata.tool_invocation_id = 'cec14b04-075e-4f47-9c24-c5e6ac7f9827'
            request_metadata.correlated_invocations_id = '9f962383-25f6-43b3-886d-56d761ac524e'

            from collections import namedtuple
            metadata_entry = namedtuple('metadata_entry', ('key', 'value'))

            return [metadata_entry(REQUEST_METADATA_HEADER_NAME,
                                   request_metadata.SerializeToString())]

    p = Peer.register_peer(peer_uid, context=DummyServicerContextWithMetadata())

    assert Peer.find_peer(peer_uid) is p
    assert p.uid == peer_uid
    assert p.tool_name == tool_name
    assert p.tool_version == tool_version


def test_peer_token():
    peer_uid = 'peer3'
    token = 'abcdef1234'

    p = Peer.register_peer(uid=peer_uid, token=token, context=context)

    assert p.uid == peer_uid
    assert p.token == token


def test_create_peer_that_exists():
    peer_uid = 'peer4'
    original_peer = Peer.register_peer(uid=peer_uid, context=context)
    new_peer = Peer.register_peer(uid=peer_uid, context=context)

    assert new_peer is original_peer


def test_peer_deregistering_the_last_instance_deletes_peer():
    peer_uid = 'peer5'
    p = Peer.register_peer(uid=peer_uid, context=context)

    assert Peer.find_peer(peer_uid) is p
    Peer.deregister_peer(peer_uid)
    assert Peer.find_peer(peer_uid) is None


def test_peer_increment_count():
    peer_uid = 'peer6'
    Peer.register_peer(uid=peer_uid, context=context)  # instance_count == 1

    Peer.register_peer(peer_uid, context=context)  # instance_count == 2
    Peer.register_peer(peer_uid, context=context)  # instance_count == 3

    Peer.deregister_peer(peer_uid)  # instance_count == 2
    Peer.deregister_peer(peer_uid)  # instance_count == 1

    assert Peer.find_peer(peer_uid) is not None
    Peer.deregister_peer(peer_uid)  # instance_count == 0
    assert Peer.find_peer(peer_uid) is None
