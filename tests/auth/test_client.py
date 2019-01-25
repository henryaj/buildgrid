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
# pylint: disable=redefined-outer-name


import os

import grpc
import pytest

from buildgrid.client.authentication import setup_channel
from buildgrid._protos.google.bytestream import bytestream_pb2, bytestream_pb2_grpc
from buildgrid.server._authentication import AuthMetadataMethod, AuthMetadataAlgorithm

from ..utils.dummy import serve_dummy
from ..utils.utils import run_in_subprocess


try:
    import jwt  # pylint: disable=unused-import
except ImportError:
    HAVE_JWT = False
else:
    HAVE_JWT = True


DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'data')

METHOD = AuthMetadataMethod.JWT
TOKEN = os.path.join(DATA_DIR, 'jwt-hs256-valid.token')
SECRET = 'your-256-bit-secret'
ALGORITHM = AuthMetadataAlgorithm.JWT_HS256


@pytest.mark.skipif(not HAVE_JWT, reason="No pyjwt")
def test_channel_token_authorization():
    # Actual test function, to be run in a subprocess:
    def __test_channel_token_authorization(queue, remote, token):
        channel, _ = setup_channel(remote, auth_token=token)
        stub = bytestream_pb2_grpc.ByteStreamStub(channel)

        request = bytestream_pb2.QueryWriteStatusRequest()
        status_code = grpc.StatusCode.OK

        try:
            next(stub.Read(request))

        except grpc.RpcError as e:
            status_code = e.code()

        queue.put(status_code)

    with serve_dummy(auth_method=METHOD, auth_secret=SECRET,
                     auth_algorithm=ALGORITHM) as server:
        status = run_in_subprocess(__test_channel_token_authorization,
                                   server.remote, TOKEN)

        assert status != grpc.StatusCode.UNAUTHENTICATED
