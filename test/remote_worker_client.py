import grpc
import platform

from google.devtools.remoteworkers.v1test2 import bots_pb2, bots_pb2_grpc, worker_pb2

class Client(object):

    def __init__(self):
        self._channel = grpc.insecure_channel('localhost:50051')
        self._stub = bots_pb2_grpc.BotsStub(self._channel)
        self._bot_session = None

    def create_bot_session(self):
        stub = bots_pb2_grpc.BotsStub(self._channel)
        
        worker = self._create_worker()
        parent = "foo_parent"

        # Unique bot ID within the farm used to identify this bot
        # Needs to be human readable.
        # All prior sessions with bot_id of same ID are invalidated.
        # If a bot attempts to update an invalid session, it must be rejected and
        # may be put in quarantine.
        bot_id = '{}.{}'.format(parent, platform.node())

        self._bot_session = bots_pb2.BotSession(worker = worker, bot_id = bot_id)

        request = bots_pb2.CreateBotSessionRequest(parent = parent,
                                                   bot_session = self._bot_session)
        response = self._stub.CreateBotSession(request)

        self._bot_session.CopyFrom(response)

    def update_bot_session(self):
        name = self._bot_session.name
        bot_session = self._bot_session
        request = bots_pb2.UpdateBotSessionRequest(name = name,
                                                   bot_session = bot_session,
                                                   update_mask = None) ## TODO: add mask
        response = self._stub.UpdateBotSession(request)
        self._bot_session.CopyFrom(response)

    def _create_worker(self):
        devices = self._create_devices()

        # Contains a list of devices and the connections between them.
        worker = worker_pb2.Worker(devices = devices)

        # Keys supported:
        # *pool
        worker.Property.key = "pool"
        worker.Property.value = "all"

        return worker

    def _create_devices(self):
        # Creates devices available to the worker
        # The first device is know as the Primary Device - the revice which
        # is running a bit and responsible to actually executing commands.
        # All other devices are known as Attatched Devices and must be controlled
        # by the Primary Device.

        # TODO: Populate devices

        devices = []

        for i in range(0, 1):
            dev = worker_pb2.Device()

            devices.append(dev)

        return devices


if __name__ == '__main__':
    client = Client()
    client.create_bot_session()
    print("Bot id: {}".format(client._bot_session.bot_id))
    print("Bot name: {}".format(client._bot_session.name))
    client.update_bot_session()
    print("Number of leases received: {}".format(len(client._bot_session.leases)))
    print("Done.")
