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

from buildgrid.server.capabilities.service import CapabilitiesService
from buildgrid.server.capabilities.instance import CapabilitiesInstance


@contextmanager
def serve_capabilities_service(instances,
                               cas_instance=None,
                               action_cache_instance=None,
                               execution_instance=None):
    server = TestServer(instances,
                        cas_instance,
                        action_cache_instance,
                        execution_instance)
    try:
        yield server
    finally:
        server.quit()


class TestServer:

    def __init__(self, instances,
                 cas_instance=None,
                 action_cache_instance=None,
                 execution_instance=None):
        self.instances = instances

        self.__queue = multiprocessing.Queue()
        self.__process = multiprocessing.Process(
            target=TestServer.serve,
            args=(self.__queue, self.instances, cas_instance, action_cache_instance, execution_instance))
        self.__process.start()

        self.port = self.__queue.get(timeout=1)
        self.remote = 'localhost:{}'.format(self.port)

    @staticmethod
    def serve(queue, instances, cas_instance, action_cache_instance, execution_instance):
        pytest_cov.embed.cleanup_on_sigterm()

        # Use max_workers default from Python 3.5+
        max_workers = (os.cpu_count() or 1) * 5
        server = grpc.server(futures.ThreadPoolExecutor(max_workers))
        port = server.add_insecure_port('localhost:0')

        capabilities_service = CapabilitiesService(server)
        for name in instances:
            capabilities_instance = CapabilitiesInstance(cas_instance, action_cache_instance, execution_instance)
            capabilities_service.add_instance(name, capabilities_instance)

        server.start()
        queue.put(port)
        signal.pause()

    def quit(self):
        if self.__process:
            self.__process.terminate()
            self.__process.join()
