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


from collections import namedtuple, OrderedDict
from datetime import datetime
from enum import Enum
import functools
import logging

import grpc

from buildgrid._exceptions import InvalidArgumentError
from buildgrid.settings import AUTH_CACHE_SIZE


try:
    import jwt
except ImportError:
    HAVE_JWT = False
else:
    HAVE_JWT = True


class AuthMetadataMethod(Enum):
    # No authentication:
    NONE = 'none'
    # JWT based authentication:
    JWT = 'jwt'


class AuthMetadataAlgorithm(Enum):
    # No encryption involved:
    UNSPECIFIED = 'unspecified'
    # JWT related algorithms:
    JWT_ES256 = 'es256'  # ECDSA signature algorithm using SHA-256 hash algorithm
    JWT_ES384 = 'es384'  # ECDSA signature algorithm using SHA-384 hash algorithm
    JWT_ES512 = 'es512'  # ECDSA signature algorithm using SHA-512 hash algorithm
    JWT_HS256 = 'hs256'  # HMAC using SHA-256 hash algorithm
    JWT_HS384 = 'hs384'  # HMAC using SHA-384 hash algorithm
    JWT_HS512 = 'hs512'  # HMAC using SHA-512 hash algorithm
    JWT_PS256 = 'ps256'  # RSASSA-PSS using SHA-256 and MGF1 padding with SHA-256
    JWT_PS384 = 'ps384'  # RSASSA-PSS signature using SHA-384 and MGF1 padding with SHA-384
    JWT_PS512 = 'ps512'  # RSASSA-PSS signature using SHA-512 and MGF1 padding with SHA-512
    JWT_RS256 = 'rs256'  # RSASSA-PKCS1-v1_5 signature algorithm using SHA-256 hash algorithm
    JWT_RS384 = 'rs384'  # RSASSA-PKCS1-v1_5 signature algorithm using SHA-384 hash algorithm
    JWT_RS512 = 'rs512'  # RSASSA-PKCS1-v1_5 signature algorithm using SHA-512 hash algorithm


class AuthContext:

    interceptor = None


class _InvalidTokenError(Exception):
    pass


class _ExpiredTokenError(Exception):
    pass


class _UnboundedTokenError(Exception):
    pass


def authorize(auth_context):
    """RPC method decorator for authorization validations.

    This decorator is design to be used together with an :class:`AuthContext`
    authorization context holder::

        @authorize(AuthContext)
        def Execute(self, request, context):

    By default, any request is accepted. Authorization validation can be
    activated by setting up a :class:`grpc.ServerInterceptor`::

        AuthContext.interceptor = AuthMetadataServerInterceptor()

    Args:
        auth_context(AuthContext): Authorization context holder.
    """
    def __authorize_decorator(behavior):
        """RPC authorization method decorator."""
        _HandlerCallDetails = namedtuple(
            '_HandlerCallDetails', ('invocation_metadata', 'method',))

        @functools.wraps(behavior)
        def __authorize_wrapper(self, request, context):
            """RPC authorization method wrapper."""
            if auth_context.interceptor is None:
                return behavior(self, request, context)

            authorized = False

            def __continuator(handler_call_details):
                nonlocal authorized
                authorized = True

            details = _HandlerCallDetails(context.invocation_metadata(),
                                          behavior.__name__)

            auth_context.interceptor.intercept_service(__continuator, details)

            if authorized:
                return behavior(self, request, context)

            context.abort(grpc.StatusCode.UNAUTHENTICATED,
                          "No valid authorization or authentication provided")

            return None

        return __authorize_wrapper

    return __authorize_decorator


class AuthMetadataServerInterceptor(grpc.ServerInterceptor):

    __auth_errors = {
        'missing-bearer': "Missing authentication header field",
        'invalid-bearer': "Invalid authentication header field",
        'invalid-token': "Invalid authentication token",
        'expired-token': "Expired authentication token",
        'unbounded-token': "Unbounded authentication token",
    }

    def __init__(self, method, secret=None, algorithm=AuthMetadataAlgorithm.UNSPECIFIED):
        """Initialises a new :class:`AuthMetadataServerInterceptor`.

        Args:
            method (AuthMetadataMethod): Type of authorization method.
            secret (str): The secret or key to be used for validating request,
                depending on `method`. Defaults to ``None``.
            algorithm (AuthMetadataAlgorithm): The crytographic algorithm used
                to encode `secret`. Defaults to ``UNSPECIFIED``.

        Raises:
            InvalidArgumentError: If `method` is not supported or if `algorithm`
                is not supported for the given `method`.
        """
        self.__logger = logging.getLogger(__name__)

        self.__bearer_cache = OrderedDict()
        self.__terminators = {}
        self.__validator = None
        self.__secret = secret

        self._method = method
        self._algorithm = algorithm

        if self._method == AuthMetadataMethod.JWT:
            self._check_jwt_support(self._algorithm)
            self.__validator = self._validate_jwt_token

        for code, message in self.__auth_errors.items():
            self.__terminators[code] = _unary_unary_rpc_terminator(message)

    # --- Public API ---

    @property
    def method(self):
        return self._method

    @property
    def algorithm(self):
        return self._algorithm

    def intercept_service(self, continuation, handler_call_details):
        try:
            # Reject requests not carrying a token:
            bearer = dict(handler_call_details.invocation_metadata)['authorization']

        except KeyError:
            self.__logger.error("Rejecting '{}' request: {}"
                                .format(handler_call_details.method.split('/')[-1],
                                        self.__auth_errors['missing-bearer']))
            return self.__terminators['missing-bearer']

        # Reject requests with malformated bearer:
        if not bearer.startswith('Bearer '):
            self.__logger.error("Rejecting '{}' request: {}"
                                .format(handler_call_details.method.split('/')[-1],
                                        self.__auth_errors['invalid-bearer']))
            return self.__terminators['invalid-bearer']

        try:
            # Hit the cache for already validated token:
            expiration_time = self.__bearer_cache[bearer]

            # Accept request if cached token hasn't expired yet:
            if expiration_time >= datetime.utcnow():
                return continuation(handler_call_details)  # Accepted

            else:
                del self.__bearer_cache[bearer]

            # Cached token has expired, reject the request:
            self.__logger.error("Rejecting '{}' request: {}"
                                .format(handler_call_details.method.split('/')[-1],
                                        self.__auth_errors['expired-token']))
            # TODO: Use grpc.Status.details to inform the client of the expiry?
            return self.__terminators['expired-token']

        except KeyError:
            pass

        assert self.__validator is not None

        try:
            # Decode and validate the new token:
            expiration_time = self.__validator(bearer[7:])

        except _InvalidTokenError as e:
            self.__logger.error("Rejecting '{}' request: {}; {}"
                                .format(handler_call_details.method.split('/')[-1],
                                        self.__auth_errors['invalid-token'], str(e)))
            return self.__terminators['invalid-token']

        except _ExpiredTokenError as e:
            self.__logger.error("Rejecting '{}' request: {}; {}"
                                .format(handler_call_details.method.split('/')[-1],
                                        self.__auth_errors['expired-token'], str(e)))
            return self.__terminators['expired-token']

        except _UnboundedTokenError as e:
            self.__logger.error("Rejecting '{}' request: {}; {}"
                                .format(handler_call_details.method.split('/')[-1],
                                        self.__auth_errors['unbounded-token'], str(e)))
            return self.__terminators['unbounded-token']

        # Cache the validated token and store expiration time:
        self.__bearer_cache[bearer] = expiration_time
        if len(self.__bearer_cache) > AUTH_CACHE_SIZE:
            self.__bearer_cache.popitem(last=False)

        return continuation(handler_call_details)  # Accepted

    # --- Private API: JWT ---

    def _check_jwt_support(self, algorithm=AuthMetadataAlgorithm.UNSPECIFIED):
        """Ensures JWT and possible dependencies are available."""
        if not HAVE_JWT:
            raise InvalidArgumentError("JWT authorization method requires PyJWT")

        try:
            if algorithm != AuthMetadataAlgorithm.UNSPECIFIED:
                jwt.register_algorithm(algorithm.value.upper(), None)

        except TypeError:
            raise InvalidArgumentError("Algorithm not supported for JWT decoding: [{}]"
                                       .format(self._algorithm))

        except ValueError:
            pass

    def _validate_jwt_token(self, token):
        """Validates a JWT token and returns its expiry date."""
        if self._algorithm != AuthMetadataAlgorithm.UNSPECIFIED:
            algorithms = [self._algorithm.value.upper()]
        else:
            algorithms = None

        try:
            payload = jwt.decode(token, self.__secret, algorithms=algorithms)

        except jwt.exceptions.ExpiredSignatureError as e:
            raise _ExpiredTokenError(e)

        except jwt.exceptions.InvalidTokenError as e:
            raise _InvalidTokenError(e)

        if 'exp' not in payload or not isinstance(payload['exp'], int):
            raise _UnboundedTokenError("Missing 'exp' in payload")

        return datetime.utcfromtimestamp(payload['exp'])


def _unary_unary_rpc_terminator(details):

    def terminate(ignored_request, context):
        context.abort(grpc.StatusCode.UNAUTHENTICATED, details)

    return grpc.unary_unary_rpc_method_handler(terminate)
