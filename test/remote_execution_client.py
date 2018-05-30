import grpc

from google.devtools.remoteexecution.v1test import remote_execution_pb2, remote_execution_pb2_grpc

class Client(object):

    def __init__(self):
        self._channel = grpc.insecure_channel('localhost:50051')

    def execute(self):
        stub = remote_execution_pb2_grpc.ExecutionStub(self._channel)

        instance_name = "testing"
        output_files = []
        action = remote_execution_pb2.Action(command_digest = None,
                                             input_root_digest = None,
                                             output_files = output_files,
                                             output_directories = None,
                                             platform = None,
                                             timeout = None,
                                             do_not_cache = True)
        skip_cache_lookup = True

        execute = remote_execution_pb2.ExecuteRequest(instance_name = instance_name,
                                                      action = action,
                                                      skip_cache_lookup = skip_cache_lookup)

        response = stub.Execute(execute)
        print("Name: {}".format(response.name))

if __name__ == '__main__':
    client = Client()
    client.execute()
