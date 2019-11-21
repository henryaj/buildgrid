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

from collections import namedtuple
import grpc
from urllib.parse import urlparse

from buildgrid.client.authentication import AuthMetadataClientInterceptor
from buildgrid.client.authentication import load_channel_authorization_token
from buildgrid.client.authentication import load_tls_channel_credentials
from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.settings import MAX_REQUEST_SIZE, REQUEST_METADATA_HEADER_NAME
from buildgrid.settings import REQUEST_METADATA_TOOL_NAME, REQUEST_METADATA_TOOL_VERSION


def setup_channel(remote_url, auth_token=None,
                  client_key=None, client_cert=None, server_cert=None,
                  action_id=None, tool_invocation_id=None,
                  correlated_invocations_id=None):
    """Creates a new gRPC client communication chanel.

    If `remote_url` does not specifies a port number, defaults 50051.

    Args:
        remote_url (str): URL for the remote, including port and protocol.
        auth_token (str): Authorization token file path.
        server_cert(str): TLS certificate chain file path.
        client_key (str): TLS root certificate file path.
        client_cert (str): TLS private key file path.
        action_id (str): Action identifier to which the request belongs to.
        tool_invocation_id (str): Identifier for a related group of Actions.
        correlated_invocations_id (str): Identifier that ties invocations together.

    Returns:
        Channel: Client Channel to be used in order to access the server
            at `remote_url`.

    Raises:
        InvalidArgumentError: On any input parsing error.
    """
    url = urlparse(remote_url)
    remote = '{}:{}'.format(url.hostname, url.port or 50051)
    details = None, None, None

    if url.scheme == 'http':
        channel = grpc.insecure_channel(remote, options=[
            ('grpc.max_send_message_length', MAX_REQUEST_SIZE),
            ('grpc.max_receive_message_length', MAX_REQUEST_SIZE),
        ])

    elif url.scheme == 'https':
        credentials, details = load_tls_channel_credentials(client_key, client_cert, server_cert)
        if not credentials:
            raise InvalidArgumentError("Given TLS details (or defaults) could be loaded")

        channel = grpc.secure_channel(remote, credentials)

    else:
        raise InvalidArgumentError("Given remote does not specify a protocol")

    request_metadata_interceptor = RequestMetadataInterceptor(
        action_id=action_id,
        tool_invocation_id=tool_invocation_id,
        correlated_invocations_id=correlated_invocations_id)

    channel = grpc.intercept_channel(channel, request_metadata_interceptor)

    if auth_token is not None:
        token = load_channel_authorization_token(auth_token)
        if not token:
            raise InvalidArgumentError("Given authorization token could be loaded")

        auth_interceptor = AuthMetadataClientInterceptor(auth_token=token)

        channel = grpc.intercept_channel(channel, auth_interceptor)

    return channel, details


class RequestMetadataInterceptor(grpc.UnaryUnaryClientInterceptor,
                                 grpc.UnaryStreamClientInterceptor,
                                 grpc.StreamUnaryClientInterceptor,
                                 grpc.StreamStreamClientInterceptor):

    def __init__(self, action_id=None, tool_invocation_id=None,
                 correlated_invocations_id=None):
        """Appends optional `RequestMetadata` header values to each call.

        Args:
            action_id (str): Action identifier to which the request belongs to.
            tool_invocation_id (str): Identifier for a related group of Actions.
            correlated_invocations_id (str): Identifier that ties invocations together.
        """
        self._action_id = action_id
        self._tool_invocation_id = tool_invocation_id
        self._correlated_invocations_id = correlated_invocations_id

        self.__header_field_name = REQUEST_METADATA_HEADER_NAME
        self.__header_field_value = self._request_metadata()

    # --- Public API ---
    def intercept_unary_unary(self, continuation, client_call_details, request):
        new_details = self._amend_call_details(client_call_details)

        return continuation(new_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        new_details = self._amend_call_details(client_call_details)

        return continuation(new_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        new_details = self._amend_call_details(client_call_details)

        return continuation(new_details, request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        new_details = self._amend_call_details(client_call_details)

        return continuation(new_details, request_iterator)

    # --- Private API ---
    def _request_metadata(self):
        """Creates a serialized RequestMetadata entry to attach to a gRPC
        call header. Arguments should be of type str or None.
        """
        request_metadata = remote_execution_pb2.RequestMetadata()
        request_metadata.tool_details.tool_name = REQUEST_METADATA_TOOL_NAME
        request_metadata.tool_details.tool_version = REQUEST_METADATA_TOOL_VERSION

        if self._action_id:
            request_metadata.action_id = self._action_id
        if self._tool_invocation_id:
            request_metadata.tool_invocation_id = self._tool_invocation_id
        if self._correlated_invocations_id:
            request_metadata.correlated_invocations_id = self._correlated_invocations_id

        return request_metadata.SerializeToString()

    def _amend_call_details(self, client_call_details):
        if client_call_details.metadata is not None:
            new_metadata = list(client_call_details.metadata)
        else:
            new_metadata = []

        new_metadata.append((self.__header_field_name,
                             self.__header_field_value))

        class _ClientCallDetails(
                namedtuple('_ClientCallDetails',
                           ('method', 'timeout', 'credentials', 'metadata',)),
                grpc.ClientCallDetails):
            pass

        return _ClientCallDetails(client_call_details.method,
                                  client_call_details.timeout,
                                  client_call_details.credentials,
                                  new_metadata)
