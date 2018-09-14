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
import tempfile

import grpc
import psutil
import pytest_cov

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.server.cas.service import ByteStreamService
from buildgrid.server.cas.service import ContentAddressableStorageService
from buildgrid.server.cas.instance import ByteStreamInstance
from buildgrid.server.cas.instance import ContentAddressableStorageInstance
from buildgrid.server.cas.storage.disk import DiskStorage
from buildgrid.utils import create_digest, merkle_tree_maker


@contextmanager
def serve_cas(instances):
    server = Server(instances)
    try:
        yield server
    finally:
        server.quit()


def kill_process_tree(pid):
    proc = psutil.Process(pid)
    children = proc.children(recursive=True)

    def kill_proc(p):
        try:
            p.kill()
        except psutil.AccessDenied:
            # Ignore this error, it can happen with
            # some setuid bwrap processes.
            pass

    # Bloody Murder
    for child in children:
        kill_proc(child)
    kill_proc(proc)


def run_in_subprocess(function, *arguments):
    queue = multiprocessing.Queue()
    # Use subprocess to avoid creation of gRPC threads in main process
    # See https://github.com/grpc/grpc/blob/master/doc/fork_support.md
    process = multiprocessing.Process(target=function,
                                      args=(queue, *arguments))

    try:
        process.start()

        result = queue.get()
        process.join()
    except KeyboardInterrupt:
        kill_process_tree(process.pid)
        raise

    return result


class Server:

    def __init__(self, instances):

        self.instances = instances

        self.__storage_path = tempfile.TemporaryDirectory()
        self.__storage = DiskStorage(self.__storage_path.name)

        self.__queue = multiprocessing.Queue()
        self.__process = multiprocessing.Process(
            target=Server.serve,
            args=(self.__queue, self.instances, self.__storage_path.name))
        self.__process.start()

        self.port = self.__queue.get()
        self.remote = 'localhost:{}'.format(self.port)

    @classmethod
    def serve(cls, queue, instances, storage_path):
        pytest_cov.embed.cleanup_on_sigterm()

        # Use max_workers default from Python 3.5+
        max_workers = (os.cpu_count() or 1) * 5
        server = grpc.server(futures.ThreadPoolExecutor(max_workers))
        port = server.add_insecure_port('localhost:0')

        storage = DiskStorage(storage_path)

        bs_service = ByteStreamService(server)
        cas_service = ContentAddressableStorageService(server)
        for name in instances:
            bs_service.add_instance(name, ByteStreamInstance(storage))
            cas_service.add_instance(name, ContentAddressableStorageInstance(storage))

        server.start()
        queue.put(port)

        signal.pause()

    def has(self, digest):
        return self.__storage.has_blob(digest)

    def get(self, digest):
        return self.__storage.get_blob(digest).read()

    def store_blob(self, blob):
        digest = create_digest(blob)
        write_buffer = self.__storage.begin_write(digest)
        write_buffer.write(blob)

        self.__storage.commit_write(digest, write_buffer)

        return digest

    def compare_blobs(self, digest, blob):
        if not self.__storage.has_blob(digest):
            return False

        stored_blob = self.__storage.get_blob(digest)
        stored_blob = stored_blob.read()

        return blob == stored_blob

    def store_message(self, message):
        message_blob = message.SerializeToString()
        message_digest = create_digest(message_blob)
        write_buffer = self.__storage.begin_write(message_digest)
        write_buffer.write(message_blob)

        self.__storage.commit_write(message_digest, write_buffer)

        return message_digest

    def compare_messages(self, digest, message):
        if not self.__storage.has_blob(digest):
            return False

        message_blob = message.SerializeToString()

        stored_blob = self.__storage.get_blob(digest)
        stored_blob = stored_blob.read()

        return message_blob == stored_blob

    def store_file(self, file_path):
        with open(file_path, 'rb') as file_bytes:
            file_blob = file_bytes.read()
        file_digest = create_digest(file_blob)
        write_buffer = self.__storage.begin_write(file_digest)
        write_buffer.write(file_blob)

        self.__storage.commit_write(file_digest, write_buffer)

        return file_digest

    def compare_files(self, digest, file_path):
        if not self.__storage.has_blob(digest):
            return False

        with open(file_path, 'rb') as file_bytes:
            file_blob = file_bytes.read()

        stored_blob = self.__storage.get_blob(digest)
        stored_blob = stored_blob.read()

        return file_blob == stored_blob

    def store_folder(self, folder_path):
        last_digest = None
        for node, blob, _ in merkle_tree_maker(folder_path):
            write_buffer = self.__storage.begin_write(node.digest)
            write_buffer.write(blob)

            self.__storage.commit_write(node.digest, write_buffer)
            last_digest = node.digest

        return last_digest

    def compare_directories(self, digest, directory_path):
        if not self.__storage.has_blob(digest):
            return False
        elif not os.path.isdir(directory_path):
            return False

        def __compare_folders(digest, path):
            directory = remote_execution_pb2.Directory()
            directory.ParseFromString(self.__storage.get_blob(digest).read())

            files, directories, symlinks = [], [], []
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False):
                    files.append(entry.name)

                elif entry.is_dir(follow_symlinks=False):
                    directories.append(entry.name)

                elif os.path.islink(entry.path):
                    symlinks.append(entry.name)

            assert len(files) == len(directory.files)
            assert len(directories) == len(directory.directories)
            assert len(symlinks) == len(directory.symlinks)

            for file_node in directory.files:
                file_path = os.path.join(path, file_node.name)

                assert file_node.name in files
                assert os.path.isfile(file_path)
                assert not os.path.islink(file_path)
                if file_node.is_executable:
                    assert os.access(file_path, os.X_OK)

                assert self.compare_files(file_node.digest, file_path)

            for directory_node in directory.directories:
                directory_path = os.path.join(path, directory_node.name)

                assert directory_node.name in directories
                assert os.path.exists(directory_path)
                assert not os.path.islink(directory_path)

                assert __compare_folders(directory_node.digest, directory_path)

            for symlink_node in directory.symlinks:
                symlink_path = os.path.join(path, symlink_node.name)

                assert symlink_node.name in symlinks
                assert os.path.islink(symlink_path)
                assert os.readlink(symlink_path) == symlink_node.target

            return True

        return __compare_folders(digest, directory_path)

    def quit(self):
        if self.__process:
            self.__process.terminate()
            self.__process.join()

        self.__storage_path.cleanup()
