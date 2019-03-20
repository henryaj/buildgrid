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


from concurrent import futures
from contextlib import contextmanager
import multiprocessing
import os
import signal

import grpc
import pytest_cov

from buildgrid.server._authentication import AuthMetadataMethod, AuthMetadataAlgorithm
from buildgrid.server._authentication import AuthMetadataServerInterceptor


@contextmanager
def serve_dummy(auth_method=AuthMetadataMethod.NONE,
                auth_secret=None, auth_algorithm=AuthMetadataAlgorithm.UNSPECIFIED):
    server = TestServer(
        auth_method=auth_method, auth_secret=auth_secret, auth_algorithm=auth_algorithm)
    try:
        yield server
    finally:
        server.quit()


class TestServer:

    def __init__(self, auth_method=AuthMetadataMethod.NONE,
                 auth_secret=None, auth_algorithm=AuthMetadataAlgorithm.UNSPECIFIED):

        self.__queue = multiprocessing.Queue()
        self.__auth_interceptor = None
        if auth_method != AuthMetadataMethod.NONE:
            self.__auth_interceptor = AuthMetadataServerInterceptor(
                method=auth_method, secret=auth_secret, algorithm=auth_algorithm)
        self.__process = multiprocessing.Process(
            target=TestServer.serve, args=(self.__queue, self.__auth_interceptor))
        self.__process.start()

        self.port = self.__queue.get()
        self.remote = 'http://localhost:{}'.format(self.port)

    @classmethod
    def serve(cls, queue, auth_interceptor):
        pytest_cov.embed.cleanup_on_sigterm()

        # Use max_workers default from Python 3.5+
        max_workers = (os.cpu_count() or 1) * 5
        executor = futures.ThreadPoolExecutor(max_workers)
        if auth_interceptor is not None:
            server = grpc.server(executor, interceptors=(auth_interceptor,))
        else:
            server = grpc.server(executor)
        port = server.add_insecure_port('localhost:0')

        queue.put(port)

        server.start()

        signal.pause()

    def quit(self):
        if self.__process:
            self.__process.terminate()
            self.__process.join()
