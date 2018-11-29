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


import base64
from collections import namedtuple
from urllib.parse import urlparse
import os

import grpc

from buildgrid._exceptions import InvalidArgumentError
from buildgrid.utils import read_file


def load_tls_channel_credentials(client_key=None, client_cert=None, server_cert=None):
    """Looks-up and loads TLS gRPC client channel credentials.

    Args:
        client_key(str, optional): Client certificate chain file path.
        client_cert(str, optional): Client private key file path.
        server_cert(str, optional): Serve root certificate file path.

    Returns:
        ChannelCredentials: Credentials to be used for a TLS-encrypted gRPC
            client channel.
    """
    if server_cert and os.path.exists(server_cert):
        server_cert_pem = read_file(server_cert)
    else:
        server_cert_pem = None
        server_cert = None

    if client_key and os.path.exists(client_key):
        client_key_pem = read_file(client_key)
    else:
        client_key_pem = None
        client_key = None

    if client_key_pem and client_cert and os.path.exists(client_cert):
        client_cert_pem = read_file(client_cert)
    else:
        client_cert_pem = None
        client_cert = None

    credentials = grpc.ssl_channel_credentials(root_certificates=server_cert_pem,
                                               private_key=client_key_pem,
                                               certificate_chain=client_cert_pem)

    return credentials, (client_key, client_cert, server_cert,)


def load_channel_authorization_token(auth_token=None):
    """Looks-up and loads client authorization token.

    Args:
        auth_token (str, optional): Token file path.

    Returns:
        str: Encoded token string.
    """
    if auth_token and os.path.exists(auth_token):
        return read_file(auth_token).decode()

    # TODO: Try loading the token from a default location?

    return None


def setup_channel(remote_url, auth_token=None,
                  client_key=None, client_cert=None, server_cert=None):
    """Creates a new gRPC client communication chanel.

    If `remote_url` does not specifies a port number, defaults 50051.

    Args:
        remote_url (str): URL for the remote, including port and protocol.
        auth_token (str): Authorization token file path.
        server_cert(str): TLS certificate chain file path.
        client_key(str): TLS root certificate file path.
        client_cert(str): TLS private key file path.

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
        channel = grpc.insecure_channel(remote)

    elif url.scheme == 'https':
        credentials, details = load_tls_channel_credentials(client_key, client_cert, server_cert)
        if not credentials:
            raise InvalidArgumentError("Given TLS details (or defaults) could be loaded")

        channel = grpc.secure_channel(remote, credentials)

    else:
        raise InvalidArgumentError("Given remote does not specify a protocol")

    if auth_token is not None:
        token = load_channel_authorization_token(auth_token)
        if not token:
            raise InvalidArgumentError("Given authorization token could be loaded")

        interpector = AuthMetadataClientInterceptor(auth_token=token)
        channel = grpc.intercept_channel(channel, interpector)

    return channel, details


class AuthMetadataClientInterceptor(
        grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor,
        grpc.StreamUnaryClientInterceptor, grpc.StreamStreamClientInterceptor):

    def __init__(self, auth_token=None, auth_secret=None):
        """Initialises a new :class:`AuthMetadataClientInterceptor`.

        Important:
            One of `auth_token` or `auth_secret` must be provided.

        Args:
            auth_token (str, optional): Authorization token as a string.
            auth_secret (str, optional): Authorization secret as a string.

        Raises:
            InvalidArgumentError: If neither `auth_token` or `auth_secret` are
                provided.
        """
        if auth_token:
            self.__secret = auth_token.strip()

        elif auth_secret:
            self.__secret = base64.b64encode(auth_secret.strip())

        else:
            raise InvalidArgumentError("A secret or token must be provided")

        self.__header_field_name = 'authorization'
        self.__header_field_value = 'Bearer {}'.format(self.__secret)

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

    def _amend_call_details(self, client_call_details):
        """Appends an authorization field to given client call details."""
        if client_call_details.metadata is not None:
            new_metadata = list(client_call_details.metadata)
        else:
            new_metadata = []

        new_metadata.append((self.__header_field_name, self.__header_field_value,))

        class _ClientCallDetails(
                namedtuple('_ClientCallDetails',
                           ('method', 'timeout', 'credentials', 'metadata')),
                grpc.ClientCallDetails):
            pass

        return _ClientCallDetails(client_call_details.method,
                                  client_call_details.timeout,
                                  client_call_details.credentials,
                                  new_metadata)
