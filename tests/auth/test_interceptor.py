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

# pylint: disable=redefined-outer-name


from collections import namedtuple
from datetime import datetime
from unittest import mock
import os
import time

import grpc
from grpc._server import _Context
import pytest

from buildgrid.server._authentication import AuthMetadataMethod, AuthMetadataAlgorithm
from buildgrid.server._authentication import AuthMetadataServerInterceptor

from ..utils.utils import read_file


try:
    import jwt  # pylint: disable=unused-import
except ImportError:
    HAVE_JWT = False
else:
    HAVE_JWT = True


DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'data')

TOKENS = [None, 'not-a-token']
SECRETS = [None, None]
ALGORITHMS = [
    AuthMetadataAlgorithm.UNSPECIFIED,
    AuthMetadataAlgorithm.UNSPECIFIED]
VALIDITIES = [False, False]
# Generic test data: token, secret, algorithm, validity:
DATA = zip(TOKENS, SECRETS, ALGORITHMS, VALIDITIES)

JWT_TOKENS = [
    'jwt-hs256-expired.token',
    'jwt-hs256-unbounded.token',
    'jwt-hs256-valid.token',
    'jwt-hs256-valid.token',

    'jwt-rs256-expired.token',
    'jwt-rs256-unbounded.token',
    'jwt-rs256-valid.token',
    'jwt-rs256-valid.token']
JWT_SECRETS = [
    'jwt-hs256-matching.secret',
    'jwt-hs256-matching.secret',
    'jwt-hs256-matching.secret',
    'jwt-hs256-conflicting.secret',

    'jwt-rs256-matching.pub.key',
    'jwt-rs256-matching.pub.key',
    'jwt-rs256-matching.pub.key',
    'jwt-rs256-conflicting.pub.key']
JWT_ALGORITHMS = [
    AuthMetadataAlgorithm.JWT_HS256,
    AuthMetadataAlgorithm.JWT_HS256,
    AuthMetadataAlgorithm.UNSPECIFIED,
    AuthMetadataAlgorithm.JWT_HS256,

    AuthMetadataAlgorithm.JWT_RS256,
    AuthMetadataAlgorithm.JWT_RS256,
    AuthMetadataAlgorithm.UNSPECIFIED,
    AuthMetadataAlgorithm.JWT_RS256]
JWT_VALIDITIES = [
    False, False, True, False,
    False, False, True, False]
# JWT test data: token, secret, algorithm, validity:
JWT_DATA = zip(JWT_TOKENS, JWT_SECRETS, JWT_ALGORITHMS, JWT_VALIDITIES)


_MockHandlerCallDetails = namedtuple(
    '_MockHandlerCallDetails', ('method', 'invocation_metadata',))
_MockMetadatum = namedtuple(
    '_MockMetadatum', ('key', 'value',))


def _mock_call_details(token, method='TestMethod'):
    invocation_metadata = [
        _MockMetadatum(
            key='user-agent',
            value='grpc-c/6.0.0 (manylinux; chttp2; gao)')]

    if token and token.count('.') == 2:
        invocation_metadata.append(_MockMetadatum(
            key='authorization', value='Bearer {}'.format(token)))

    elif token:
        invocation_metadata.append(_MockMetadatum(
            key='authorization', value=token))

    return _MockHandlerCallDetails(
        method=method, invocation_metadata=invocation_metadata)


def _unary_unary_rpc_terminator(details):

    def terminate(ignored_request, context):
        context.set_code(grpc.StatusCode.OK)

    return grpc.unary_unary_rpc_method_handler(terminate)


@pytest.mark.parametrize('token,secret,algorithm,validity', DATA)
def test_authorization(token, secret, algorithm, validity):
    interceptor = AuthMetadataServerInterceptor(
        method=AuthMetadataMethod.NONE, secret=secret, algorithm=algorithm)

    call_details = _mock_call_details(token)
    context = mock.create_autospec(_Context, spec_set=True)

    try:
        handler = interceptor.intercept_service(None, call_details)

    except AssertionError:
        context.set_code(grpc.StatusCode.OK)

    else:
        handler.unary_unary(None, context)

    if validity:
        context.set_code.assert_called_once_with(grpc.StatusCode.OK)
        context.abort.assert_not_called()

    else:
        context.abort.assert_called_once_with(grpc.StatusCode.UNAUTHENTICATED, mock.ANY)
        context.set_code.assert_not_called()


@pytest.mark.skipif(not HAVE_JWT, reason="No pyjwt")
@pytest.mark.parametrize('token,secret,algorithm,validity', JWT_DATA)
def test_jwt_authorization(token, secret, algorithm, validity):
    token = read_file(os.path.join(DATA_DIR, token), text_mode=True).strip()
    secret = read_file(os.path.join(DATA_DIR, secret), text_mode=True).strip()

    interceptor = AuthMetadataServerInterceptor(
        method=AuthMetadataMethod.JWT, secret=secret, algorithm=algorithm)

    continuator = _unary_unary_rpc_terminator
    call_details = _mock_call_details(token)
    context = mock.create_autospec(_Context, spec_set=True)

    handler = interceptor.intercept_service(continuator, call_details)
    handler.unary_unary(None, context)

    if validity:
        context.set_code.assert_called_once_with(grpc.StatusCode.OK)
        context.abort.assert_not_called()

    else:
        context.abort.assert_called_once_with(grpc.StatusCode.UNAUTHENTICATED, mock.ANY)
        context.set_code.assert_not_called()

    # Token should have been cached now, let's test authorization again:
    context = mock.create_autospec(_Context, spec_set=True)

    handler = interceptor.intercept_service(continuator, call_details)
    handler.unary_unary(None, context)

    if validity:
        context.set_code.assert_called_once_with(grpc.StatusCode.OK)
        context.abort.assert_not_called()

    else:
        context.abort.assert_called_once_with(grpc.StatusCode.UNAUTHENTICATED, mock.ANY)
        context.set_code.assert_not_called()


@pytest.mark.skipif(not HAVE_JWT, reason="No pyjwt")
def test_jwt_authorization_expiry():
    secret, algorithm = 'your-256-bit-secret', AuthMetadataAlgorithm.JWT_HS256
    now = int(datetime.utcnow().timestamp())
    payload = {'sub': 'BuildGrid Expiry Test', 'iat': now, 'exp': now + 2}
    token = jwt.encode(payload, secret, algorithm=algorithm.value.upper()).decode()

    interceptor = AuthMetadataServerInterceptor(
        method=AuthMetadataMethod.JWT, secret=secret, algorithm=algorithm)

    # First, test generated token validation:
    continuator = _unary_unary_rpc_terminator
    call_details = _mock_call_details(token)
    context = mock.create_autospec(_Context, spec_set=True)

    handler = interceptor.intercept_service(continuator, call_details)
    handler.unary_unary(None, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.OK)
    context.abort.assert_not_called()

    # Second, ensure cached token validation:
    context = mock.create_autospec(_Context, spec_set=True)

    handler = interceptor.intercept_service(continuator, call_details)
    handler.unary_unary(None, context)

    context.set_code.assert_called_once_with(grpc.StatusCode.OK)
    context.abort.assert_not_called()

    # Then wait for the token to expire:
    time.sleep(3)

    # Finally, test for cached-token invalidation:
    context = mock.create_autospec(_Context, spec_set=True)

    handler = interceptor.intercept_service(continuator, call_details)
    handler.unary_unary(None, context)

    context.abort.assert_called_once_with(grpc.StatusCode.UNAUTHENTICATED, mock.ANY)
    context.set_code.assert_not_called()
