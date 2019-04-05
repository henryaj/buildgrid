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


from concurrent import futures
from contextlib import contextmanager
import multiprocessing
import os
import signal

import grpc
import pytest_cov

from buildgrid.server.actioncache.service import ActionCacheService
from buildgrid.server.actioncache.instance import ActionCache
from buildgrid.server.capabilities.service import CapabilitiesService
from buildgrid.server.capabilities.instance import CapabilitiesInstance
from buildgrid.server.cas.storage.remote import RemoteStorage

from .cas import serve_cas


@contextmanager
def serve_cache(instances, allow_updates=True):
    server = TestServer(instances, allow_updates)
    try:
        yield server
    finally:
        server.quit()


class TestServer:

    def __init__(self, instances, allow_updates):
        self.instances = instances

        self.__queue = multiprocessing.Queue()
        self.__process = multiprocessing.Process(
            target=TestServer.serve,
            args=(self.__queue, self.instances, allow_updates))
        self.__process.start()

        self.port = self.__queue.get()
        self.remote = 'localhost:{}'.format(self.port)

    @classmethod
    def serve(cls, queue, instances, allow_updates):
        pytest_cov.embed.cleanup_on_sigterm()

        # Use max_workers default from Python 3.5+
        max_workers = (os.cpu_count() or 1) * 5
        server = grpc.server(futures.ThreadPoolExecutor(max_workers))
        port = server.add_insecure_port('localhost:0')

        with serve_cas(['testing']) as cas:
            channel = grpc.insecure_channel(cas.remote)
            storage = RemoteStorage(channel, 'testing')

            ac_service = ActionCacheService(server)
            capabilities_service = CapabilitiesService(server)
            for name in instances:
                ac_instance = ActionCache(storage, 256, allow_updates=allow_updates)
                capabilities_instance = CapabilitiesInstance(action_cache_instance=ac_instance)
                capabilities_service.add_instance(name, capabilities_instance)
                ac_service.add_instance(name, ac_instance)

            server.start()
            queue.put(port)

            signal.pause()

    def quit(self):
        if self.__process:
            self.__process.terminate()
            self.__process.join()
