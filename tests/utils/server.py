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


from contextlib import contextmanager
import multiprocessing
import signal

import pytest_cov

from buildgrid._app.settings import parser
from buildgrid.server.instance import Server


@contextmanager
def serve(configuration):
    server = TestServer(configuration)
    try:
        yield server
    finally:
        server.quit()


class TestServer:

    def __init__(self, configuration):

        self.configuration = configuration

        self.__queue = multiprocessing.Queue()
        self.__process = multiprocessing.Process(
            target=TestServer.serve,
            args=(self.__queue, self.configuration))
        self.__process.start()

        self.port = self.__queue.get()
        self.remote = 'localhost:{}'.format(self.port)

    @classmethod
    def serve(cls, queue, configuration):
        pytest_cov.embed.cleanup_on_sigterm()

        server = Server()

        def __signal_handler(signum, frame):
            server.stop()

        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, __signal_handler)

        instances = parser.get_parser().safe_load(configuration)['instances']
        for instance in instances:
            instance_name = instance['name']
            services = instance['services']
            for service in services:
                service.register_instance_with_server(instance_name, server)

        port = server.add_port('localhost:0', None)

        queue.put(port)

        server.start()

    def quit(self):
        if self.__process:
            self.__process.terminate()
            self.__process.join()
