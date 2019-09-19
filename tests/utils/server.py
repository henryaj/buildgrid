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
import os
import signal
import tempfile

import pytest_cov

from buildgrid._app.settings import parser
from buildgrid.server.instance import Server
from buildgrid.server._monitoring import MonitoringOutputType, MonitoringOutputFormat


@contextmanager
def serve(configuration, monitor=False):
    _, path = tempfile.mkstemp()
    try:
        server = TestServer(configuration, monitor, path)
        yield server, path
    finally:
        server.quit()
        os.remove(path)


class TestServer:

    def __init__(self, configuration, monitor=False, endpoint_location=None):

        self.configuration = configuration

        self.__queue = multiprocessing.Queue()
        self.__process = multiprocessing.Process(
            target=TestServer.serve,
            args=(self.__queue, self.configuration, monitor, endpoint_location))
        self.__process.start()

        self.port = self.__queue.get()
        self.remote = 'localhost:{}'.format(self.port)

    @classmethod
    def serve(cls, queue, configuration, monitor, endpoint_location):
        pytest_cov.embed.cleanup_on_sigterm()

        server = Server(monitor=monitor,
                        mon_endpoint_type=MonitoringOutputType.FILE,
                        mon_endpoint_location=endpoint_location,
                        mon_serialisation_format=MonitoringOutputFormat.STATSD)

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
