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
import uuid

import grpc
import pytest_cov

from buildgrid._enums import LeaseState
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid.server.bots import service


@contextmanager
def serve_bots_interface(instances):
    server = TestServer(instances)
    try:
        yield server
    finally:
        server.quit()


# Use subprocess to avoid creation of gRPC threads in main process
# See https://github.com/grpc/grpc/blob/master/doc/fork_support.md
class TestServer:

    def __init__(self, instances):
        self.instances = instances

        self.__queue = multiprocessing.Queue()
        # Queue purely for bot session updates
        self.__bot_session_queue = multiprocessing.Queue()
        # Queue to send messages to subprocess
        self.__message_queue = multiprocessing.Queue()
        self.__process = multiprocessing.Process(
            target=TestServer.serve,
            args=(self.__queue, self.instances,
                  self.__bot_session_queue, self.__message_queue))
        self.__process.start()

        self.port = self.__queue.get()
        self.remote = 'localhost:{}'.format(self.port)

    @staticmethod
    def serve(queue, instances, bot_session_queue, message_queue):
        pytest_cov.embed.cleanup_on_sigterm()

        # Use max_workers default from Python 3.5+
        max_workers = (os.cpu_count() or 1) * 5
        server = grpc.server(futures.ThreadPoolExecutor(max_workers))
        port = server.add_insecure_port('localhost:0')

        bots_service = service.BotsService(server)
        for name in instances:
            bots_interface = BotsInterface(bot_session_queue, message_queue)
            bots_service.add_instance(name, bots_interface)

        server.start()
        queue.put(port)
        signal.pause()

    def get_bot_session(self, timeout=1):
        bot_session = bots_pb2.BotSession()
        bot_session.ParseFromString(self.__bot_session_queue.get(timeout=timeout))
        return bot_session

    # Injects leases
    def inject_work(self, lease=None, timeout=1):
        if not lease:
            lease = bots_pb2.Lease()
            lease.state = LeaseState.PENDING.value

        lease_string = lease.SerializeToString()
        self.__message_queue.put(('INJECT_WORK', lease_string))

    # Triggers a cancellation of a lease from server
    def cancel_lease(self, lease_id):
        self.__message_queue.put(('CANCEL_LEASE', lease_id))

    def quit(self):
        if self.__process:
            self.__process.terminate()
            self.__process.join()


class BotsInterface:

    def __init__(self, bot_session_queue, message_queue):
        self.__bot_session_queue = bot_session_queue
        self.__message_queue = message_queue

    def register_instance_with_server(self, instance_name, server):
        server.add_bots_interface(self, instance_name)

    def create_bot_session(self, parent, bot_session):
        name = "{}/{}".format(parent, str(uuid.uuid4()))
        bot_session.name = name

        while not self.__message_queue.empty():
            message = self.__message_queue.get()
            if message[0] == 'INJECT_WORK':
                lease_string = message[1]
                lease = bots_pb2.Lease()
                lease.ParseFromString(lease_string)
                bot_session.leases.extend([lease])

        self.__bot_session_queue.put(bot_session.SerializeToString())
        return bot_session

    def update_bot_session(self, name, bot_session, deadline=None):
        for lease in bot_session.leases:
            state = LeaseState(lease.state)
            if state == LeaseState.COMPLETED:
                lease.Clear()

            elif state == LeaseState.CANCELLED:
                lease.Clear()

        while not self.__message_queue.empty():
            message = self.__message_queue.get()

            if message[0] == 'INJECT_WORK':
                lease_string = message[1]
                lease = bots_pb2.Lease()
                lease.ParseFromString(lease_string)
                bot_session.leases.extend([lease])

            elif message[0] == 'CANCEL_LEASE':
                lease_id = message[1]
                for lease in bot_session.leases:
                    if lease.id == lease_id:
                        lease.state = LeaseState.CANCELLED.value

        self.__bot_session_queue.put(bot_session.SerializeToString())
        return bot_session
