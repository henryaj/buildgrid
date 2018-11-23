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

# pylint: disable=redefined-outer-name


from copy import deepcopy
import os
import tempfile

import grpc
import pytest

from buildgrid.client.cas import download, upload
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.utils import create_digest

from ..utils.cas import serve_cas
from ..utils.utils import run_in_subprocess


INTANCES = ['', 'instance']
BLOBS = [(b'',), (b'test-string',), (b'test', b'string')]
MESSAGES = [
    (remote_execution_pb2.Directory(),),
    (remote_execution_pb2.SymlinkNode(name='name', target='target'),),
    (remote_execution_pb2.Action(do_not_cache=True),
     remote_execution_pb2.ActionResult(exit_code=12))
]
DATA_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'data')
FILES = [
    (os.path.join(DATA_DIR, 'void'),),
    (os.path.join(DATA_DIR, 'hello.cc'),),
    (os.path.join(DATA_DIR, 'hello', 'hello.c'),
     os.path.join(DATA_DIR, 'hello', 'hello.h'))]
FOLDERS = [
    (os.path.join(DATA_DIR, 'hello'),)]
DIRECTORIES = [
    (os.path.join(DATA_DIR, 'hello'),),
    (os.path.join(DATA_DIR, 'hello'), DATA_DIR)]


@pytest.mark.parametrize('blobs', BLOBS)
@pytest.mark.parametrize('instance', INTANCES)
def test_upload_blob(instance, blobs):
    # Actual test function, to be run in a subprocess:
    def __test_upload_blob(queue, remote, instance, blobs):
        # Open a channel to the remote CAS server:
        channel = grpc.insecure_channel(remote)

        digests = []
        with upload(channel, instance) as uploader:
            if len(blobs) > 1:
                for blob in blobs:
                    digest = uploader.put_blob(blob, queue=True)
                    digests.append(digest.SerializeToString())
            else:
                digest = uploader.put_blob(blobs[0], queue=False)
                digests.append(digest.SerializeToString())

        queue.put(digests)

    # Start a minimal CAS server in a subprocess:
    with serve_cas([instance]) as server:
        digests = run_in_subprocess(__test_upload_blob,
                                    server.remote, instance, blobs)

        for blob, digest_blob in zip(blobs, digests):
            digest = remote_execution_pb2.Digest()
            digest.ParseFromString(digest_blob)

            assert server.has(digest)
            assert server.compare_blobs(digest, blob)


@pytest.mark.parametrize('messages', MESSAGES)
@pytest.mark.parametrize('instance', INTANCES)
def test_upload_message(instance, messages):
    # Actual test function, to be run in a subprocess:
    def __test_upload_message(queue, remote, instance, messages):
        # Open a channel to the remote CAS server:
        channel = grpc.insecure_channel(remote)

        digests = []
        with upload(channel, instance) as uploader:
            if len(messages) > 1:
                for message in messages:
                    digest = uploader.put_message(message, queue=True)
                    digests.append(digest.SerializeToString())
            else:
                digest = uploader.put_message(messages[0], queue=False)
                digests.append(digest.SerializeToString())

        queue.put(digests)

    # Start a minimal CAS server in a subprocess:
    with serve_cas([instance]) as server:
        digests = run_in_subprocess(__test_upload_message,
                                    server.remote, instance, messages)

        for message, digest_blob in zip(messages, digests):
            digest = remote_execution_pb2.Digest()
            digest.ParseFromString(digest_blob)

            assert server.has(digest)
            assert server.compare_messages(digest, message)


@pytest.mark.parametrize('file_paths', FILES)
@pytest.mark.parametrize('instance', INTANCES)
def test_upload_file(instance, file_paths):
    # Actual test function, to be run in a subprocess:
    def __test_upload_file(queue, remote, instance, file_paths):
        # Open a channel to the remote CAS server:
        channel = grpc.insecure_channel(remote)

        digests = []
        with upload(channel, instance) as uploader:
            if len(file_paths) > 1:
                for file_path in file_paths:
                    digest = uploader.upload_file(file_path, queue=True)
                    digests.append(digest.SerializeToString())
            else:
                digest = uploader.upload_file(file_paths[0], queue=False)
                digests.append(digest.SerializeToString())

        queue.put(digests)

    # Start a minimal CAS server in a subprocess:
    with serve_cas([instance]) as server:
        digests = run_in_subprocess(__test_upload_file,
                                    server.remote, instance, file_paths)

        for file_path, digest_blob in zip(file_paths, digests):
            digest = remote_execution_pb2.Digest()
            digest.ParseFromString(digest_blob)

            assert server.has(digest)
            assert server.compare_files(digest, file_path)


@pytest.mark.parametrize('directory_paths', DIRECTORIES)
@pytest.mark.parametrize('instance', INTANCES)
def test_upload_directory(instance, directory_paths):
    # Actual test function, to be run in a subprocess:
    def __test_upload_directory(queue, remote, instance, directory_paths):
        # Open a channel to the remote CAS server:
        channel = grpc.insecure_channel(remote)

        digests = []
        with upload(channel, instance) as uploader:
            if len(directory_paths) > 1:
                for directory_path in directory_paths:
                    digest = uploader.upload_directory(directory_path, queue=True)
                    digests.append(digest.SerializeToString())
            else:
                digest = uploader.upload_directory(directory_paths[0], queue=False)
                digests.append(digest.SerializeToString())

        queue.put(digests)

    # Start a minimal CAS server in a subprocess:
    with serve_cas([instance]) as server:
        digests = run_in_subprocess(__test_upload_directory,
                                    server.remote, instance, directory_paths)

        for directory_path, digest_blob in zip(directory_paths, digests):
            digest = remote_execution_pb2.Digest()
            digest.ParseFromString(digest_blob)

            assert server.compare_directories(digest, directory_path)


@pytest.mark.parametrize('directory_paths', DIRECTORIES)
@pytest.mark.parametrize('instance', INTANCES)
def test_upload_tree(instance, directory_paths):
    # Actual test function, to be run in a subprocess:
    def __test_upload_tree(queue, remote, instance, directory_paths):
        # Open a channel to the remote CAS server:
        channel = grpc.insecure_channel(remote)

        digests = []
        with upload(channel, instance) as uploader:
            if len(directory_paths) > 1:
                for directory_path in directory_paths:
                    digest = uploader.upload_tree(directory_path, queue=True)
                    digests.append(digest.SerializeToString())
            else:
                digest = uploader.upload_tree(directory_paths[0], queue=False)
                digests.append(digest.SerializeToString())

        queue.put(digests)

    # Start a minimal CAS server in a subprocess:
    with serve_cas([instance]) as server:
        digests = run_in_subprocess(__test_upload_tree,
                                    server.remote, instance, directory_paths)

        for directory_path, digest_blob in zip(directory_paths, digests):
            digest = remote_execution_pb2.Digest()
            digest.ParseFromString(digest_blob)

            assert server.has(digest)

            tree = remote_execution_pb2.Tree()
            tree.ParseFromString(server.get(digest))

            directory_digest = create_digest(tree.root.SerializeToString())

            assert server.compare_directories(directory_digest, directory_path)


@pytest.mark.parametrize('blobs', BLOBS)
@pytest.mark.parametrize('instance', INTANCES)
def test_download_blob(instance, blobs):
    # Actual test function, to be run in a subprocess:
    def __test_download_blob(queue, remote, instance, digests):
        # Open a channel to the remote CAS server:
        channel = grpc.insecure_channel(remote)

        blobs = []
        with download(channel, instance) as downloader:
            if len(digests) > 1:
                blobs.extend(downloader.get_blobs(digests))
            else:
                blobs.append(downloader.get_blob(digests[0]))

        queue.put(blobs)

    # Start a minimal CAS server in a subprocess:
    with serve_cas([instance]) as server:
        digests = []
        for blob in blobs:
            digest = server.store_blob(blob)
            digests.append(digest)

        blobs = run_in_subprocess(__test_download_blob,
                                  server.remote, instance, digests)

        for digest, blob in zip(digests, blobs):
            assert server.compare_blobs(digest, blob)


@pytest.mark.parametrize('messages', MESSAGES)
@pytest.mark.parametrize('instance', INTANCES)
def test_download_message(instance, messages):
    # Actual test function, to be run in a subprocess:
    def __test_download_message(queue, remote, instance, digests, empty_messages):
        # Open a channel to the remote CAS server:
        channel = grpc.insecure_channel(remote)

        messages = []
        with download(channel, instance) as downloader:
            if len(digests) > 1:
                messages = downloader.get_messages(digests, empty_messages)
                messages = list([m.SerializeToString() for m in messages])
            else:
                message = downloader.get_message(digests[0], empty_messages[0])
                messages.append(message.SerializeToString())

        queue.put(messages)

    # Start a minimal CAS server in a subprocess:
    with serve_cas([instance]) as server:
        empty_messages, digests = [], []
        for message in messages:
            digest = server.store_message(message)
            digests.append(digest)

            empty_message = deepcopy(message)
            empty_message.Clear()
            empty_messages.append(empty_message)

        messages = run_in_subprocess(__test_download_message,
                                     server.remote, instance, digests, empty_messages)

        for digest, message_blob, message in zip(digests, messages, empty_messages):
            message.ParseFromString(message_blob)

            assert server.compare_messages(digest, message)


@pytest.mark.parametrize('file_paths', FILES)
@pytest.mark.parametrize('instance', INTANCES)
def test_download_file(instance, file_paths):
    # Actual test function, to be run in a subprocess:
    def __test_download_file(queue, remote, instance, digests, paths):
        # Open a channel to the remote CAS server:
        channel = grpc.insecure_channel(remote)

        with download(channel, instance) as downloader:
            if len(digests) > 1:
                for digest, path in zip(digests, paths):
                    downloader.download_file(digest, path, queue=False)
            else:
                downloader.download_file(digests[0], paths[0], queue=False)

        queue.put(None)

    # Start a minimal CAS server in a subprocess:
    with serve_cas([instance]) as server:
        with tempfile.TemporaryDirectory() as temp_folder:
            paths, digests = [], []
            for file_path in file_paths:
                digest = server.store_file(file_path)
                digests.append(digest)

                path = os.path.relpath(file_path, start=DATA_DIR)
                path = os.path.join(temp_folder, path)
                paths.append(path)

                run_in_subprocess(__test_download_file,
                                  server.remote, instance, digests, paths)

            for digest, path in zip(digests, paths):
                assert server.compare_files(digest, path)


@pytest.mark.parametrize('folder_paths', FOLDERS)
@pytest.mark.parametrize('instance', INTANCES)
def test_download_directory(instance, folder_paths):
    # Actual test function, to be run in a subprocess:
    def __test_download_directory(queue, remote, instance, digests, paths):
        # Open a channel to the remote CAS server:
        channel = grpc.insecure_channel(remote)

        with download(channel, instance) as downloader:
            if len(digests) > 1:
                for digest, path in zip(digests, paths):
                    downloader.download_directory(digest, path)
            else:
                downloader.download_directory(digests[0], paths[0])

        queue.put(None)

    # Start a minimal CAS server in a subprocess:
    with serve_cas([instance]) as server:
        with tempfile.TemporaryDirectory() as temp_folder:
            paths, digests = [], []
            for folder_path in folder_paths:
                digest = server.store_folder(folder_path)
                digests.append(digest)

                path = os.path.relpath(folder_path, start=DATA_DIR)
                path = os.path.join(temp_folder, path)
                paths.append(path)

                run_in_subprocess(__test_download_directory,
                                  server.remote, instance, digests, paths)

            for digest, path in zip(digests, paths):
                assert server.compare_directories(digest, path)
